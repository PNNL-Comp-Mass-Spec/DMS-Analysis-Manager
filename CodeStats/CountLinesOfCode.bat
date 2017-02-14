@echo off

if exist CodeStats.txt del CodeStats.txt

echo Using cloc to count lines of code (requires Strawberry Perl)
echo.
rem Download cloc from https://github.com/AlDanial/cloc
rem Download Strawberry Perl from http://strawberryperl.com/

echo AM_Program
echo. >> CodeStats.txt
echo Stats for AM_Program >> CodeStats.txt
C:\Strawberry\perl\bin\perl.exe cloc-1.72.pl ..\AM_Program --exclude-ext=XML,vbproj,csproj,bat --exclude-dir=obj,_Trash,_Unused,AM_Shared --quiet >> CodeStats.txt

echo AM_Shared
echo. >> CodeStats.txt
echo Stats for AM_Shared >> CodeStats.txt
C:\Strawberry\perl\bin\perl.exe cloc-1.72.pl ..\AM_Program\AM_Shared --exclude-ext=XML,vbproj,csproj,bat --exclude-dir=obj,_Trash,_Unused --quiet >> CodeStats.txt

for /D %%i in (..\Plugins\*) do call CountLinesOfCodeForPlugin.bat %%i

set SedExe="C:\Program Files (x86)\Gow\bin\sed.exe"
if exist %SedExe% goto ParseResults

set SedExe="C:\gnuwin32\bin\sed.exe"
if exist %SedExe% goto ParseResults

echo Could not find sed.exe
echo Unable to remove extra text from CodeStats.txt

goto Done

:ParseResults

echo.
echo Using %SedExe%

type CodeStats.txt  | %SedExe% -r "s/..\\//" > CodeStats2.txt
type CodeStats2.txt | %SedExe% -r "s/(github\.com\/AlDanial\/cloc v *[0-9.]+).+/\1/" > CodeStats3.txt

del CodeStats.txt
del CodeStats2.txt

move CodeStats3.txt CodeStats.txt

echo.
echo Done processing; see file CodeStats.txt

:Done
