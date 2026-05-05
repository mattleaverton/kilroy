# Navigation test — push into detail screen and pop back
Output Tests/tui/output/navigation.gif
Set Shell "bash"
Set FontSize 14
Set Width 120
Set Height 40
Set Padding 10

Type "OMNIUI_DEMO_ANIM=0 $(swift build --show-bin-path 2>/dev/null)/KitchenSink --notcurses"
Enter
Sleep 2s

# Tab to "Open details" (12 tabs)
Tab@100ms 12
Sleep 0.5s

# Push to detail screen
Enter
Sleep 1s

# Should see "Detail screen (level 1)"

# Go back via Escape
Escape
Sleep 1s

# Should be back on main screen

# Quit
Type "q"
Sleep 1s
