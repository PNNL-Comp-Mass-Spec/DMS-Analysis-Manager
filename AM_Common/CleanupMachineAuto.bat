@echo off
echo.
echo.

pushd \\%1\c$\

echo.
echo.
echo -- Warning: not showing the existing files in the DMS_WorkDir folders --
echo -- Auto-deleting all files and starting the managers                  --
echo -- No other pause messages will be shown                              --
echo.
pause

echo.
echo.
echo Deleting files in the DMS_Work folders:
del \dms_workdir1\*.* /q
del \dms_workdir2\*.* /q

echo.
echo.
echo Files/Folders still in DMS_WorkDir1:
dir \dms_workdir1\*.* /b

echo.
echo.
echo Files/Folders still in DMS_WorkDir2:
dir \dms_workdir2\*.*  /b

echo.
echo.
echo If any folders are present, you need to manually delete them
echo Jump to folder using:
echo pushd \\%1\c$\DMS_WorkDir1\
echo pushd \\%1\c$\DMS_WorkDir2\

echo.
echo.
echo Deleting Flag Files:
cd \dms_programs\
cd dms5
del flagfile.txt /s
del flagFile_Svr.txt /s
popd

echo.
echo.
echo Starting progrunner
psservice \\%1 start progrunner
