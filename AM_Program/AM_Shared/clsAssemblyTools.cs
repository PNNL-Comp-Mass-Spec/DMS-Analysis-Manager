
using System;
using System.Diagnostics;
using System.IO;

//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

namespace AnalysisManagerBase
{

    /// <summary>
    /// Tools for manipulating and documenting the assemblies used for each analysis job
    /// </summary>
    public class clsAssemblyTools
    {

        #region "Methods"

        public void GetLoadedAssemblyInfo(ref clsSummaryFile objSummaryFile)
        {
            var currentDomain = AppDomain.CurrentDomain;

            // Make an array for the list of assemblies.
            var assemblies = currentDomain.GetAssemblies();

            // List the assemblies in the current application domain.
            Console.WriteLine("List of assemblies loaded in current appdomain:");
            foreach (var item in assemblies)
            {
                objSummaryFile.Add(item.ToString());
            }
        }

        public void GetComponentFileVersionInfo(clsSummaryFile objSummaryFile)
        {
            // Create a reference to the current directory.
            var di = new DirectoryInfo(clsGlobal.GetAppFolderPath());

            // Create an array representing the files in the current directory.
            var dllFiles = di.GetFiles("*.dll");

            // get file version info for files
            foreach (var dllFile in dllFiles)
            {
                var versionInfo = FileVersionInfo.GetVersionInfo(dllFile.FullName);

                var strFileInfo = "File:             " + dllFile.FullName + Environment.NewLine;

                if (!string.IsNullOrWhiteSpace(versionInfo.InternalName) && versionInfo.InternalName != dllFile.Name)
                {
                    strFileInfo += "InternalName:     " + versionInfo.InternalName + Environment.NewLine;
                }

                if (versionInfo.InternalName != versionInfo.OriginalFilename)
                {
                    strFileInfo += "OriginalFilename: " + versionInfo.OriginalFilename + Environment.NewLine;
                }

                if (!string.IsNullOrWhiteSpace(versionInfo.ProductName))
                {
                    strFileInfo += "Product:          " + versionInfo.ProductName + Environment.NewLine;
                }

                strFileInfo += "ProductVersion:   " + versionInfo.ProductVersion + Environment.NewLine;

                if (versionInfo.FileVersion != versionInfo.ProductVersion)
                {
                    strFileInfo += "FileVersion:      " + versionInfo.FileVersion + Environment.NewLine;
                }

                if (!string.IsNullOrWhiteSpace(versionInfo.FileDescription) && versionInfo.FileDescription != versionInfo.ProductName)
                {
                    strFileInfo += "FileDescription:  " + versionInfo.FileDescription + Environment.NewLine;
                }

                objSummaryFile.Add(strFileInfo);
            }

        }

        #endregion

    }
}