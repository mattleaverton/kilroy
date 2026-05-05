# Picker test — expand picker, navigate options, select
# Initial focus is on [+]. Focus order: [+], [-], Toggle, TextField, Picker
# Need 4 Tabs to reach Picker.

xdotool key --window "$WID" --clearmodifiers Tab; sleep 0.1  # [-]
xdotool key --window "$WID" --clearmodifiers Tab; sleep 0.1  # Toggle
xdotool key --window "$WID" --clearmodifiers Tab; sleep 0.1  # TextField
xdotool key --window "$WID" --clearmodifiers Tab; sleep 0.3  # Picker

# Screenshot: Picker focused
import -window root "$OUTPUT_DIR/picker_focused.png" 2>/dev/null || true

# Press Enter to expand the picker dropdown
xdotool key --window "$WID" --clearmodifiers Return
sleep 1

# Screenshot: Picker expanded with options visible
import -window root "$OUTPUT_DIR/picker_expanded.png" 2>/dev/null || true

# Navigate down to next option
xdotool key --window "$WID" --clearmodifiers Down; sleep 0.3
import -window root "$OUTPUT_DIR/picker_option_down.png" 2>/dev/null || true

# Select the option with Enter
xdotool key --window "$WID" --clearmodifiers Return; sleep 0.5
import -window root "$OUTPUT_DIR/picker_selected.png" 2>/dev/null || true
