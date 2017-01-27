@echo off

if exist CodeStats.txt del CodeStats.txt

echo Using cloc to count lines of code (requires Strawberry Perl)
echo Download cloc from https://github.com/AlDanial/cloc
echo.

echo. >> CodeStats.txt
echo Stats for AM_Program >> CodeStats.txt
C:\Strawberry\perl\bin\perl.exe ..\..\..\CountLinesOfCode\cloc-1.72.pl ..\AM_Program --exclude-ext=XML,vbproj,csproj,bat --exclude-dir=obj,_Trash,_Unused,AM_Shared --quiet >> CodeStats.txt

echo. >> CodeStats.txt
echo Stats for AM_Shared >> CodeStats.txt
C:\Strawberry\perl\bin\perl.exe ..\..\..\CountLinesOfCode\cloc-1.72.pl ..\AM_Program\AM_Shared --exclude-ext=XML,vbproj,csproj,bat --exclude-dir=obj,_Trash,_Unused --quiet >> CodeStats.txt

for /D %%i in (..\Plugins\*) do echo %%i && echo. >> CodeStats.txt && echo Stats for %%i >> CodeStats.txt && C:\Strawberry\perl\bin\perl.exe ..\..\..\CountLinesOfCode\cloc-1.72.pl %%i  --exclude-ext=XML,vbproj,csproj,bat --exclude-dir=obj,_Trash,_Unused,MSGFResultsSummarizerExe --quiet >> CodeStats.txt

type CodeStats.txt  | sed -r "s/..\\//" > CodeStats2.txt
type CodeStats2.txt | sed -r "s/(github\.com\/AlDanial\/cloc v *[0-9.]+).+/\1/" > CodeStats3.txt

del CodeStats.txt
del CodeStats2.txt

move CodeStats3.txt CodeStats.txt
