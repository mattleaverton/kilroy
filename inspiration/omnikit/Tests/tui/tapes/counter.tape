# Counter test — increment counter via Tab+Enter, verify state change
Output Tests/tui/output/counter.gif
Set Shell "bash"
Set FontSize 14
Set Width 120
Set Height 40
Set Padding 10

Type "OMNIUI_DEMO_ANIM=0 $(swift build --show-bin-path 2>/dev/null)/KitchenSink --notcurses"
Enter
Sleep 2s

# Tab to [+] button
Tab
Sleep 0.3s

# Increment 3 times
Enter
Sleep 0.3s
Enter
Sleep 0.3s
Enter
Sleep 1s

# Counter should show 3

# Quit
Type "q"
Sleep 1s
