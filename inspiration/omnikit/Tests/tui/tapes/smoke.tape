# Smoke test — verify KitchenSink starts and renders via notcurses
Output Tests/tui/output/smoke.gif
Set Shell "bash"
Set FontSize 14
Set Width 120
Set Height 40
Set Padding 10

Type "OMNIUI_SMOKE_SECONDS=3 OMNIUI_DEMO_ANIM=0 $(swift build --show-bin-path 2>/dev/null)/KitchenSink --notcurses"
Enter
Sleep 5s
