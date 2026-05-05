# Navigation test — push into detail screen, interact, go back
# Initial focus is on [+]. Focus order:
#   [+], [-], Toggle, TextField, Picker, Pick(0), Pick(1),
#   Model+1, Model-1, [Model name], Observed+1, Open details
# Need 11 Tabs to reach "Open details".

for i in $(seq 1 11); do
    xdotool key --window "$WID" --clearmodifiers Tab
    sleep 0.1
done
sleep 0.3

# Press Enter to navigate to detail screen
xdotool key --window "$WID" --clearmodifiers Return
sleep 1

# Screenshot: detail screen
import -window root "$OUTPUT_DIR/navigation_detail.png" 2>/dev/null || true

# Tab to "Local +1" (Back → Local +1 = 2 tabs)
xdotool key --window "$WID" --clearmodifiers Tab; sleep 0.1
xdotool key --window "$WID" --clearmodifiers Tab; sleep 0.2

# Increment local twice
xdotool key --window "$WID" --clearmodifiers Return; sleep 0.2
xdotool key --window "$WID" --clearmodifiers Return; sleep 0.5
import -window root "$OUTPUT_DIR/navigation_local_inc.png" 2>/dev/null || true

# Tab to "Push next" and push to level 2
xdotool key --window "$WID" --clearmodifiers Tab; sleep 0.2
xdotool key --window "$WID" --clearmodifiers Return; sleep 1
import -window root "$OUTPUT_DIR/navigation_level2.png" 2>/dev/null || true

# Escape back to level 1, then main
xdotool key --window "$WID" --clearmodifiers Escape; sleep 0.5
xdotool key --window "$WID" --clearmodifiers Escape; sleep 0.5
