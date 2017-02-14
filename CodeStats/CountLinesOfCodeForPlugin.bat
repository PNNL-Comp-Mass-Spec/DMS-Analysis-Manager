@echo off

if "%1" == "..\Plugins\_Trash" Goto SkipFolder
if "%1" == "..\Plugins\_Unused" Goto SkipFolder

echo %1

echo. >> CodeStats.txt

echo Stats for %1 >> CodeStats.txt

C:\Strawberry\perl\bin\perl.exe cloc-1.72.pl %1  --exclude-ext=XML,vbproj,csproj,bat --exclude-dir=obj,_Trash,_Unused,MSGFResultsSummarizerExe --quiet >> CodeStats.txt

Goto Done

:SkipFolder
echo Skip folder %1

:Done
