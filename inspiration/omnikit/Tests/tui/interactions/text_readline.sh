# Readline shortcuts test for TextField (Ctrl+A / Ctrl+E / Ctrl+K).
# Initial focus is on [+]. Focus order: [+], [-], Toggle, TextField.

xdotool key --window "$WID" --clearmodifiers Tab; sleep 0.1  # [-]
xdotool key --window "$WID" --clearmodifiers Tab; sleep 0.1  # Toggle
xdotool key --window "$WID" --clearmodifiers Tab; sleep 0.3  # TextField

xdotool type --window "$WID" --clearmodifiers --delay 30 "abcdef"
sleep 0.2

xdotool key --window "$WID" --clearmodifiers ctrl+a; sleep 0.1
xdotool type --window "$WID" --clearmodifiers --delay 30 "X"
sleep 0.1

xdotool key --window "$WID" --clearmodifiers ctrl+e; sleep 0.1
xdotool type --window "$WID" --clearmodifiers --delay 30 "Z"
sleep 0.1

xdotool key --window "$WID" --clearmodifiers ctrl+a; sleep 0.1
xdotool key --window "$WID" --clearmodifiers Right; sleep 0.05
xdotool key --window "$WID" --clearmodifiers Right; sleep 0.05
xdotool key --window "$WID" --clearmodifiers Right; sleep 0.05
xdotool key --window "$WID" --clearmodifiers ctrl+k; sleep 0.4

# Move focus away from the text field before final capture so cursor blink timing
# doesn't introduce screenshot noise.
xdotool key --window "$WID" --clearmodifiers Tab; sleep 0.2
