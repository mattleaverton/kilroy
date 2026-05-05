# Counter increment test
# Initial focus is already on [+] (first focusable element).
# No Tab needed — just press Enter to activate [+].

# Press Enter 3 times to increment counter to 3
xdotool key --window "$WID" --clearmodifiers Return; sleep 0.2
xdotool key --window "$WID" --clearmodifiers Return; sleep 0.2
xdotool key --window "$WID" --clearmodifiers Return; sleep 0.5

# Screenshot: Count: 3
import -window root "$OUTPUT_DIR/counter_increment_at3.png" 2>/dev/null || true

# Tab to [-] and decrement once → Count: 2
xdotool key --window "$WID" --clearmodifiers Tab; sleep 0.2
xdotool key --window "$WID" --clearmodifiers Return; sleep 0.5
