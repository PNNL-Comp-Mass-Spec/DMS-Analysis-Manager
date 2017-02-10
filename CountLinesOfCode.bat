rem Obtain Count Lines of Code from https://github.com/AlDanial/cloc
rem Obtain Strawberry Perl from http://strawberryperl.com/

C:\Strawberry\perl\bin\perl.exe cloc-1.72.pl Plugins --exclude-ext=XML,vbproj,csproj,bat --exclude-dir=obj,_Trash,_Unused

C:\Strawberry\perl\bin\perl.exe cloc-1.72.pl AM_Program --exclude-ext=XML,vbproj,csproj,bat --exclude-dir=obj,_Trash,_Unused

pause
