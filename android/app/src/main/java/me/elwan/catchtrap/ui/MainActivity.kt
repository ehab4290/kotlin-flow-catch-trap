package me.elwan.catchtrap.ui

import android.graphics.Typeface
import android.os.Bundle
import android.view.View
import android.widget.Button
import android.widget.CheckBox
import android.widget.LinearLayout
import android.widget.RadioButton
import android.widget.RadioGroup
import android.widget.ScrollView
import android.widget.TextView
import androidx.activity.ComponentActivity
import androidx.activity.viewModels
import androidx.core.content.edit
import androidx.core.view.ViewCompat
import androidx.core.view.WindowInsetsCompat
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.repeatOnLifecycle
import kotlinx.coroutines.launch
import me.elwan.catchtrap.CallStyle
import me.elwan.catchtrap.CatchTrapApp
import me.elwan.catchtrap.Host
import me.elwan.catchtrap.NetworkSetup
import me.elwan.catchtrap.ProfileUiState
import me.elwan.catchtrap.Scenario
import me.elwan.catchtrap.measure.Measurement

/**
 * One screen, built in code so the project has no resources to maintain.
 *
 * Tap a scenario and the ViewModel runs it for real. When the upstream error skips `catch`, the app
 * crashes and Logcat shows FATAL EXCEPTION, because nothing here catches it.
 */
class MainActivity : ComponentActivity() {

    private val repository by lazy { (application as CatchTrapApp).repository }
    private val viewModel: ProfileViewModel by viewModels { ProfileViewModel.factory(repository) }

    private val hostButtons = LinkedHashMap<Host, Int>()
    private val callStyleButtons = LinkedHashMap<CallStyle, Int>()
    private lateinit var hostGroup: RadioGroup
    private lateinit var callStyleGroup: RadioGroup

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val prefs = getPreferences(MODE_PRIVATE)
        val savedSetup = NetworkSetup(
            host = Host.valueOf(prefs.getString(KEY_HOST, Host.UNRESOLVABLE.name)!!),
            callStyle = CallStyle.valueOf(prefs.getString(KEY_CALL_STYLE, CallStyle.SUSPENDING.name)!!),
        )

        // Cold start: begin observing before the first frame is drawn, the way a ViewModel's init
        // block does. The main thread is busy for a while after this, which is when values get lost.
        val observedAtLaunch = savedInstanceState == null && prefs.getBoolean(KEY_OBSERVE_AT_LAUNCH, false)
        if (observedAtLaunch) viewModel.run(Scenario.OBSERVE, savedSetup)

        val column = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(dp(16), dp(16), dp(16), dp(16))
        }
        val scroll = ScrollView(this).apply { addView(column) }
        ViewCompat.setOnApplyWindowInsetsListener(scroll) { view, insets ->
            val bars = insets.getInsets(
                WindowInsetsCompat.Type.systemBars() or WindowInsetsCompat.Type.displayCutout()
            )
            view.setPadding(bars.left, bars.top, bars.right, bars.bottom)
            insets
        }

        column.addView(text("Flow.catch after a channel", sizeSp = 20f, bold = true))
        column.addView(text("Every button runs real code: Room, Retrofit, viewModelScope. A crash is a real crash."))

        column.addView(label("Network host"))
        hostGroup = RadioGroup(this)
        for (host in Host.entries) {
            val button = RadioButton(this).apply { id = View.generateViewId(); text = host.label }
            hostButtons[host] = button.id
            hostGroup.addView(button)
        }
        hostGroup.check(hostButtons.getValue(savedSetup.host))
        hostGroup.setOnCheckedChangeListener { _, _ -> prefs.edit { putString(KEY_HOST, currentSetup().host.name) } }
        column.addView(hostGroup)

        column.addView(label("Network call"))
        callStyleGroup = RadioGroup(this)
        for (style in CallStyle.entries) {
            val button = RadioButton(this).apply { id = View.generateViewId(); text = style.label }
            callStyleButtons[style] = button.id
            callStyleGroup.addView(button)
        }
        callStyleGroup.check(callStyleButtons.getValue(savedSetup.callStyle))
        callStyleGroup.setOnCheckedChangeListener { _, _ ->
            prefs.edit { putString(KEY_CALL_STYLE, currentSetup().callStyle.name) }
        }
        column.addView(callStyleGroup)

        column.addView(label("catch after flowOn"))
        for (scenario in Scenario.entries.filter { !it.fixed }) column.addView(scenarioButton(scenario))

        column.addView(label("catch before flowOn (the fix)"))
        for (scenario in Scenario.entries.filter { it.fixed }) column.addView(scenarioButton(scenario))

        column.addView(label("Cold start"))
        column.addView(CheckBox(this).apply {
            text = "Run observe() during the next cold start"
            isChecked = prefs.getBoolean(KEY_OBSERVE_AT_LAUNCH, false)
            setOnCheckedChangeListener { _, checked -> prefs.edit { putBoolean(KEY_OBSERVE_AT_LAUNCH, checked) } }
        })

        column.addView(label(if (observedAtLaunch) "States given to the screen (observe() ran at launch)" else "States given to the screen"))
        val statesView = text("", monospace = true)
        column.addView(statesView)

        column.addView(label("Measure"))
        val measureView = text("", monospace = true)
        column.addView(Button(this).apply {
            text = "Run every scenario $MEASURE_RUNS times and count"
            isAllCaps = false
            setOnClickListener { button ->
                button.isEnabled = false
                measureView.text = "Measuring. The uncaught-exception handler is replaced while this runs, so nothing crashes."
                val setup = currentSetup()
                lifecycleScope.launch {
                    val lines = StringBuilder("${setup.host.label}\n${setup.callStyle.label}\n")
                    for (scenario in Scenario.entries) {
                        val row = Measurement.measure(repository, scenario, setup, MEASURE_RUNS)
                        lines.append('\n').append(row.summary())
                        measureView.text = lines.toString()
                    }
                    button.isEnabled = true
                }
            }
        })
        column.addView(measureView)

        setContentView(scroll)

        lifecycleScope.launch {
            repeatOnLifecycle(Lifecycle.State.STARTED) {
                viewModel.states.collect { states -> statesView.text = render(states) }
            }
        }
    }

    private fun currentSetup() = NetworkSetup(
        host = hostButtons.entries.first { it.value == hostGroup.checkedRadioButtonId }.key,
        callStyle = callStyleButtons.entries.first { it.value == callStyleGroup.checkedRadioButtonId }.key,
    )

    private fun scenarioButton(scenario: Scenario) = Button(this).apply {
        text = scenario.title
        isAllCaps = false
        setOnClickListener { viewModel.run(scenario, currentSetup()) }
    }

    private fun render(states: List<ProfileUiState>): String =
        if (states.isEmpty()) "(nothing yet)"
        else states.withIndex().joinToString("\n") { (index, state) ->
            when (state) {
                is ProfileUiState.Content -> "${index + 1}. Content: ${state.profile.name} [${state.profile.origin}]"
                is ProfileUiState.Error -> "${index + 1}. Error: ${state.message}"
            }
        }

    private fun text(value: String, sizeSp: Float = 14f, bold: Boolean = false, monospace: Boolean = false) =
        TextView(this).apply {
            text = value
            textSize = sizeSp
            if (bold) setTypeface(typeface, Typeface.BOLD)
            if (monospace) typeface = Typeface.MONOSPACE
            setPadding(0, dp(4), 0, dp(4))
        }

    private fun label(value: String) = text(value, sizeSp = 13f, bold = true).apply {
        setPadding(0, dp(20), 0, dp(4))
    }

    private fun dp(value: Int): Int = (value * resources.displayMetrics.density).toInt()

    private companion object {
        const val KEY_HOST = "host"
        const val KEY_CALL_STYLE = "callStyle"
        const val KEY_OBSERVE_AT_LAUNCH = "observeAtLaunch"
        const val MEASURE_RUNS = 40
    }
}
