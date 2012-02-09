This folder contains Xcalibur files required to support Decon2LS and DeconMSN 
processing Finnigan data files. It also contains the DeconMSN executable, 
which needs to be in this directory to find all the necessary support files.

When installing an analysis manager that will handle these files, copy the 
files in this folder to C:\XCalDLL on the analysis machine; run 
registerFiles.bat to update the Windows registry, and set the analysis 
manager's "lcqdtaloc" key to point to the directory these files are located in.

You should also copy the XcalDLL_ExtractMSn folder to C:\XcalDLL_ExtractMSn to 
support Extract_MSn.exe