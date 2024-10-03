using System;
using System.Diagnostics;
using System.IO;

//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

namespace AnalysisManagerBase.JobConfig
{
    /// <summary>
    /// Tools for manipulating and documenting the assemblies used for each analysis job
    /// </summary>
    public class AssemblyTools
    {
        /// <summary>
        /// Call summaryFile.Add for each loaded assembly
        /// </summary>
        /// <param name="summaryFile">Summary file instance</param>
        [Obsolete("Unused")]
        private void GetLoadedAssemblyInfo(SummaryFile summaryFile)
        {
            var currentDomain = AppDomain.CurrentDomain;

            // Make an array for the list of assemblies.
            var assemblies = currentDomain.GetAssemblies();

            // List the assemblies in the current application domain.
            Console.WriteLine("List of assemblies loaded in current application domain:");

            foreach (var item in assemblies)
            {
                summaryFile.Add(item.ToString());
            }
        }

        /// <summary>
        /// Call summaryFile.Add for each DLL in the application directory
        /// </summary>
        /// <param name="summaryFile">Summary file instance</param>
        public void GetComponentFileVersionInfo(SummaryFile summaryFile)
        {
            // Create a reference to the current directory.
            var di = new DirectoryInfo(Global.GetAppDirectoryPath());

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

                summaryFile.Add(fileInfo);
            }
        }
    }
}