This folder contains source code and required dll's for analysis manager and plug-in development. Copies of all separately-developed dll's are to be kept in the AM_Common directory.

For deployment of an Analysis Manager:

1) Copy the AnalysisManager.xml, AnalysisManagerProg.exe, and plugin_info.xml files from the AM_Program/bin folder to a folder on the analysis machine.

2) Copy all files in the AM_Common folder to the same folder on the analysis machine that the AnalysisManagerProg.exe was copied to.

3) Edit the AnalysisManager.xml and plugin_info.xml files as appropriate for the analysis type and machine configuration.

4) Start the manager by running AnalysisManagerProg.exe on the analysis machine.

NOTE: For Sequest and XTandem analysis, either ICR2LS must be installed on the analysis machine, or a copy of icr2ls32.dll must be placed in the folder on the analysis machine where AnalysisManagerProg.exe is located.