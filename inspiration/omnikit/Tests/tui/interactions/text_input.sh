# Text input test
# Initial focus is on [+]. Focus order: [+], [-], Toggle, TextField
# Need 3 Tabs to reach TextField.

xdotool key --window "$WID" --clearmodifiers Tab; sleep 0.1  # [-]
xdotool key --window "$WID" --clearmodifiers Tab; sleep 0.1  # Toggle
xdotool key --window "$WID" --clearmodifiers Tab; sleep 0.3  # TextField

# Type text (avoid 'q' which quits when not in text editing mode)
xdotool type --window "$WID" --clearmodifiers --delay 40 "OmniUI"
sleep 0.5

# Screenshot: text field shows "OmniUI", greeting shows "Hello, OmniUI"
import -window root "$OUTPUT_DIR/text_input_typed.png" 2>/dev/null || true

# Test backspace
xdotool key --window "$WID" --clearmodifiers BackSpace; sleep 0.2
xdotool key --window "$WID" --clearmodifiers BackSpace; sleep 0.5

# Move focus away from the text field before final capture so cursor blink timing
# doesn't introduce screenshot noise.
xdotool key --window "$WID" --clearmodifiers Tab; sleep 0.2
