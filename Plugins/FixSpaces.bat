@echo off
echo About to preview files that need tabs switched to spaces
echo Analyzing all .vb and .cs files for the analysis manager plugins
echo Include /Apply when calling TabsToSpaces.exe to actually update the files
echo.
pause

echo.
..\..\..\TabsToSpaces\bin\TabsToSpaces.exe *.vb /excludefolders:.vs,_Trash,_Unused /s

echo.
..\..\..\TabsToSpaces\bin\TabsToSpaces.exe *.cs /excludefolders:.vs,_Trash,_Unused /s

echo.
pause
