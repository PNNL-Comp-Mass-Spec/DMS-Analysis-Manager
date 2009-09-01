@echo off
echo.
echo.

pushd \\%1\c$\

goto PreviewFiles

echo.
echo.
echo -- Warning: not showing the existing files in the DMS_WorkDir folders --
echo.
goto SkipPreviewFiles

:PreviewFiles
echo.
echo.
echo Files in DMS_WorkDir1:
dir \dms_workdir1\*.* | more /e

echo.
echo.
echo Files in DMS_WorkDir2:
dir \dms_workdir2\*.* | more /e

:SkipPreviewFiles
echo.
echo.
echo About to delete files:
pause
del \dms_workdir1\*.*
del \dms_workdir2\*.*

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
echo If any folders are present, you need to delete them
echo Jump to folder using:
echo pushd \\%1\c$\DMS_WorkDir1\
echo pushd \\%1\c$\DMS_WorkDir2\
pause

echo.
echo.
echo Deleting Flag Files:
cd \dms_programs\
del flagfile.txt /s
del flagFile_Svr.txt /s
popd

echo.
echo.
echo About to start progrunner
pause
psservice \\%1 start progrunner