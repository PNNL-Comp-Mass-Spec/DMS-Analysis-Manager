This program reads the GlyQ-IQ results file (Dataset_iqResults_.txt) for a given job, 
summarizes the results, and calls PostJobResults to store the results in DMS

Program syntax:
GlyQResultsSummarizer.exe
 GlyQResultsFilePath.txt /Job:JobNumber

The first parameter is the path to a tab-delimited text file with the GlyQ results to parse

Use /Job to specify the Job Number for this results file

If processing a file tracked in DMS, you can use /Job:Auto and this program will
automatically determine the Job number from the parent folder name.  This will
only work if the parent folder is of the form GLY201503151614_Auto1170022


-------------------------------------------------------------------------------
Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
Copyright 2015, Battelle Memorial Institute.  All Rights Reserved.

E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com
Website: http://panomics.pnnl.gov/ or http://omics.pnl.gov
-------------------------------------------------------------------------------

Licensed under the Apache License, Version 2.0; you may not use this file except 
in compliance with the License.  You may obtain a copy of the License at 
http://www.apache.org/licenses/LICENSE-2.0
