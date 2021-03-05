//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2007, Battelle Memorial Institute
// Created 12/19/2007
//
//*********************************************************************************************************

using AnalysisManagerBase;
using PRISM;
using System;
using System.IO;
using System.Text.RegularExpressions;
using System.Xml;

namespace AnalysisManagerProg
{
    /// <summary>
    /// Class for loading analysis manager plugins
    /// </summary>
    public class clsPluginLoader : EventNotifier
    {
        #region "Member variables"

        private readonly string mMgrFolderPath;

        private readonly clsSummaryFile mSummaryFile;

        #endregion

        #region "Properties"

        /// <summary>
        /// Plugin config file name
        /// </summary>
        /// <remarks>Defaults to plugin_info.xml</remarks>
        public string FileName { get; set; } = "plugin_info.xml";

        /// <summary>
        /// When true, show additional messages at the console
        /// </summary>
        public bool TraceMode { get; set; }

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="summaryFile"></param>
        /// <param name="MgrFolderPath"></param>
        public clsPluginLoader(clsSummaryFile summaryFile, string MgrFolderPath)
        {
            mSummaryFile = summaryFile;
            mMgrFolderPath = MgrFolderPath;
        }

#if PLUGIN_DEBUG_MODE_ENABLED

        private IToolRunner DebugModeGetToolRunner(string className)
        {
            IToolRunner myToolRunner = null;

            switch (className)
#pragma warning disable 1522
            {
#pragma warning restore 1522
                //case "AnalysisManagerTopFDPlugIn.clsAnalysisToolRunnerTopFD":
                //    myToolRunner = new AnalysisManagerTopFDPlugIn.clsAnalysisToolRunnerTopFD();
                //    break;
                //case "AnalysisManagerTopPICPlugIn.clsAnalysisToolRunnerTopPIC":
                //    myToolRunner = new AnalysisManagerTopPICPlugIn.clsAnalysisToolRunnerTopPIC();
                //    break;
            }

            // ReSharper disable once ExpressionIsAlwaysNull
            return myToolRunner;
        }

        private IAnalysisResources DebugModeGetAnalysisResources(string className)
        {
            IAnalysisResources myModule = null;

            switch (className)
#pragma warning disable 1522
            {
#pragma warning restore 1522
                //case "AnalysisManagerTopFDPlugIn.clsAnalysisResourcesTopFD":
                //    myModule = new AnalysisManagerTopFDPlugIn.clsAnalysisResourcesTopFD();
                //    break;
                //case "AnalysisManagerTopPICPlugIn.clsAnalysisResourcesTopPIC":
                //    myModule = new AnalysisManagerTopPICPlugIn.clsAnalysisResourcesTopPIC();
                //    break;
            }

            // ReSharper disable once ExpressionIsAlwaysNull
            return myModule;
        }
#endif

        /// <summary>
        /// Retrieves data for specified plugin from plugin info config file
        /// </summary>
        /// <param name="xpath">XPath spec for specified plugin</param>
        /// <param name="className">Name of class for plugin (return value) </param>
        /// <param name="assemblyName">Name of assembly for plugin (return value)</param>
        /// <returns>TRUE for success, FALSE for failure</returns>
        private bool GetPluginInfo(string xpath, out string className, out string assemblyName)
        {
            var doc = new XmlDocument();
            var pluginInfo = string.Empty;

            className = string.Empty;
            assemblyName = string.Empty;

            try
            {
                if (string.IsNullOrEmpty(xpath))
                {
                    throw new ArgumentException("XPath must be defined", nameof(xpath));
                }

                // ReSharper disable once StringLiteralTypo
                pluginInfo = "XPath=\"" + xpath + "\"; className=\"" + className + "\"; assyName=" + assemblyName + "\"";

                //read the tool runner info file
                doc.Load(GetPluginInfoFilePath(FileName));
                var root = doc.DocumentElement;

                if (root == null)
                {
                    throw new Exception("Valid XML not found in file " + FileName);
                }

                // find the element that matches the tool name
                var nodeList = root.SelectNodes(xpath);

                if (nodeList == null)
                {
                    throw new Exception(string.Format("XPath did not have a match for '{0}' in {1}", pluginInfo, FileName));
                }

                // make sure that we found exactly one element,
                // and if we did, retrieve its information
                if (nodeList.Count != 1)
                {
                    var testToolMatcher = new Regex("@Tool='(?<TestToolName>test_(?<AltToolName>[a-z_-]+))'", RegexOptions.IgnoreCase);
                    var match = testToolMatcher.Match(pluginInfo);

                    if (match.Success)
                    {
                        OnWarningEvent(string.Format("Could not resolve tool name '{0}'; will try '{1}'",
                                                     match.Groups["TestToolName"], match.Groups["AltToolName"]));
                        return false;
                    }
                    throw new Exception("Could not resolve tool name; " + pluginInfo);
                }

                foreach (XmlElement n in nodeList)
                {
                    className = n.GetAttribute("Class");
                    assemblyName = n.GetAttribute("AssemblyFile");
                }
                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in GetPluginInfo:" + ex.Message + "; " + pluginInfo, ex);
                return false;
            }
        }

        /// <summary>
        /// Loads the specified dll
        /// </summary>
        /// <param name="className">Name of class to load (from GetPluginInfo)</param>
        /// <param name="assemblyName">Name of assembly to load (from GetPluginInfo)</param>
        /// <returns>An object referencing the specified dll</returns>
        private object LoadObject(string className, string assemblyName)
        {
            try
            {
                // Build instance of tool runner subclass from class name and assembly file name.
                var pluginInfoFilePath = GetPluginInfoFilePath(assemblyName);

                // Make sure the file exists and is capitalized correctly
                var pluginInfoFile = new FileInfo(pluginInfoFilePath);
                var expectedName = Path.GetFileName(pluginInfoFilePath);

                if (!pluginInfoFile.Exists)
                {
                    var pluginFolder = new DirectoryInfo(mMgrFolderPath);
                    var nameUpdated = false;
                    foreach (var file in pluginFolder.GetFiles("*"))
                    {
                        if (!string.Equals(file.Name, expectedName, StringComparison.OrdinalIgnoreCase))
                            continue;

                        OnDebugEvent("Filename case mismatch; auto-correcting to switch from " +
                                     expectedName + " to " + file.Name);
                        pluginInfoFile = file;
                        nameUpdated = true;
                        break;
                    }

                    if (!nameUpdated)
                    {
                        OnErrorEvent("Plugin file not found: " + pluginInfoFilePath);
                        return null;
                    }
                }

                if (TraceMode)
                    OnDebugEvent("Call System.Reflection.Assembly.LoadFrom for assembly " + PathUtils.CompactPathString(pluginInfoFile.FullName, 60));

                var assembly = System.Reflection.Assembly.LoadFrom(pluginInfoFile.FullName);

                var assemblyType = assembly.GetType(className, false, true);

                var instance = Activator.CreateInstance(assemblyType);
                return instance;
            }
            catch (Exception ex)
            {
                // Cache exceptions
                OnErrorEvent(string.Format("clsPluginLoader.LoadObject(), for class {0}, assembly {1}", className, assemblyName), ex);
                return null;
            }
        }

        /// <summary>
        /// Loads a tool runner object
        /// </summary>
        /// <param name="toolName">Name of tool</param>
        /// <returns>An object meeting the IToolRunner interface</returns>
        public IToolRunner GetToolRunner(string toolName)
        {
            var xpath = "//ToolRunners/ToolRunner[@Tool='" + toolName.ToLower() + "']";

            IToolRunner myToolRunner = null;

            if (GetPluginInfo(xpath, out var className, out var assemblyName))
            {
#if PLUGIN_DEBUG_MODE_ENABLED
                // This constant is defined on the Build tab of the Analysis Manager solution
                myToolRunner = DebugModeGetToolRunner(className);
                if (myToolRunner != null)
                {
                    return myToolRunner;
                }
#endif

                var newToolRunner = LoadObject(className, assemblyName);
                if (newToolRunner != null)
                {
                    try
                    {
                        myToolRunner = (IToolRunner)newToolRunner;
                    }
                    catch (Exception ex)
                    {
                        OnErrorEvent(string.Format("clsPluginLoader.GetToolRunner(), for class {0}, assembly {1}", className, assemblyName), ex);
                    }
                }
                mSummaryFile.Add("Loaded ToolRunner: " + className + " from " + assemblyName);
            }
            else
            {
                mSummaryFile.Add("Unable to load ToolRunner for " + toolName);
            }

            return myToolRunner;
        }

        /// <summary>
        /// Loads a resourcer object
        /// </summary>
        /// <param name="toolName">Name of analysis tool</param>
        /// <returns>An object meeting the IAnalysisResources interface</returns>
        public IAnalysisResources GetAnalysisResources(string toolName)
        {
            var xpath = "//Resourcers/Resourcer[@Tool='" + toolName.ToLower() + "']";

            IAnalysisResources myModule = null;

            if (GetPluginInfo(xpath, out var className, out var assemblyName))
            {
#if PLUGIN_DEBUG_MODE_ENABLED
                // This constant is defined on the Build tab of the Analysis Manager solution
                myModule = DebugModeGetAnalysisResources(className);
                if (myModule != null)
                {
                    return myModule;
                }
#endif

                var newResourcer = LoadObject(className, assemblyName);
                if (newResourcer != null)
                {
                    try
                    {
                        myModule = (IAnalysisResources)newResourcer;
                    }
                    catch (Exception ex)
                    {
                        OnErrorEvent(string.Format("clsPluginLoader.GetAnalysisResources(), for class {0}, assembly {1}", className, assemblyName), ex);
                    }
                }
                mSummaryFile.Add("Loaded resourcer: " + className + " from " + assemblyName);
            }
            else
            {
                mSummaryFile.Add("Unable to load resourcer for " + toolName);
            }

            return myModule;
        }

        /// <summary>
        /// Gets the path to the plugin info config file
        /// </summary>
        /// <param name="pluginInfoFileName">Name of plugin info file</param>
        /// <returns>Path to plugin info file</returns>
        private string GetPluginInfoFilePath(string pluginInfoFileName)
        {
            return Path.Combine(mMgrFolderPath, pluginInfoFileName);
        }
        #endregion
    }
}
