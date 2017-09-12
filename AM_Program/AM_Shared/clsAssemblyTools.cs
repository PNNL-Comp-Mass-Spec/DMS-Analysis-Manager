
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

        /// <summary>
        /// Call objSummaryFile.Add for each loaded assembly
        /// </summary>
        /// <param name="objSummaryFile"></param>
        [Obsolete("Unused")]
        private void GetLoadedAssemblyInfo(clsSummaryFile objSummaryFile)
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

        /// <summary>
        /// Call objSummaryFile.Add for each DLL in the application directory
        /// </summary>
        /// <param name="objSummaryFile"></param>
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

                var fileInfo = "File:             " + dllFile.FullName + Environment.NewLine;

                if (!string.IsNullOrWhiteSpace(versionInfo.InternalName) && versionInfo.InternalName != dllFile.Name)
                {
                    fileInfo += "InternalName:     " + versionInfo.InternalName + Environment.NewLine;
                }

                if (versionInfo.InternalName != versionInfo.OriginalFilename)
                {
                    fileInfo += "OriginalFilename: " + versionInfo.OriginalFilename + Environment.NewLine;
                }

                if (!string.IsNullOrWhiteSpace(versionInfo.ProductName))
                {
                    fileInfo += "Product:          " + versionInfo.ProductName + Environment.NewLine;
                }

                fileInfo += "ProductVersion:   " + versionInfo.ProductVersion + Environment.NewLine;

                if (versionInfo.FileVersion != versionInfo.ProductVersion)
                {
                    fileInfo += "FileVersion:      " + versionInfo.FileVersion + Environment.NewLine;
                }

                if (!string.IsNullOrWhiteSpace(versionInfo.FileDescription) && versionInfo.FileDescription != versionInfo.ProductName)
                {
                    fileInfo += "FileDescription:  " + versionInfo.FileDescription + Environment.NewLine;
                }

                objSummaryFile.Add(fileInfo);
            }

        }

        #endregion

    }
}