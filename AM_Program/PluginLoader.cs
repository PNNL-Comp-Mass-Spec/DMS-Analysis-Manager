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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Xml;
using System.Xml.Linq;

namespace AnalysisManagerProg
{
    /// <summary>
    /// Class for loading analysis manager plugins
    /// </summary>
    public class PluginLoader : EventNotifier
    {
        // Ignore Spelling: Resourcers

        #region "Member variables"

        private readonly string mMgrFolderPath;

        private readonly SummaryFile mSummaryFile;

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
        public PluginLoader(SummaryFile summaryFile, string MgrFolderPath)
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
                //case "AnalysisManagerTopFDPlugIn.AnalysisToolRunnerTopFD":
                //    myToolRunner = new AnalysisManagerTopFDPlugIn.AnalysisToolRunnerTopFD();
                //    break;
                //case "AnalysisManagerTopPICPlugIn.AnalysisToolRunnerTopPIC":
                //    myToolRunner = new AnalysisManagerTopPICPlugIn.AnalysisToolRunnerTopPIC();
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
                //case "AnalysisManagerTopFDPlugIn.AnalysisResourcesTopFD":
                //    myModule = new AnalysisManagerTopFDPlugIn.AnalysisResourcesTopFD();
                //    break;
                //case "AnalysisManagerTopPICPlugIn.AnalysisResourcesTopPIC":
                //    myModule = new AnalysisManagerTopPICPlugIn.AnalysisResourcesTopPIC();
                //    break;
            }

            // ReSharper disable once ExpressionIsAlwaysNull
            return myModule;
        }
#endif

        /// <summary>
        /// Retrieves data for specified plugin from plugin info config file
        /// </summary>
        /// <param name="classType">Either Resourcer or ToolRunner</param>
        /// <param name="toolName"></param>
        /// <param name="nodeNameHierarchy">List of node names to traverse to reach the ToolRunner or Resourcer element for the current plugin</param>
        /// <param name="className">Output: name of class for plugin</param>
        /// <param name="assemblyName">Output: name of assembly for plugin</param>
        /// <returns>TRUE for success, FALSE for failure</returns>
        private bool GetPluginInfo(
            string classType,
            string toolName,
            List<string> nodeNameHierarchy,
            out string className,
            out string assemblyName)
        {
            className = string.Empty;
            assemblyName = string.Empty;
            var pluginInfoFilePath = GetPluginInfoFilePath(FileName);

            try
            {
                if (nodeNameHierarchy.Count == 0)
                {
                    throw new ArgumentException("nodeNameHierarchy cannot be empty", nameof(nodeNameHierarchy));
                }

                var pluginInfoFile = new FileInfo(pluginInfoFilePath);
                if (!pluginInfoFile.Exists)
                {
                    OnErrorEvent("PluginInfo file not found: " + pluginInfoFile.FullName);
                    return false;
                }

                using var reader = new StreamReader(new FileStream(pluginInfoFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                // Note that XDocument supersedes XmlDocument and XPathDocument
                // XDocument can often be easier to use since XDocument is LINQ-based

                var doc = XDocument.Parse(reader.ReadToEnd());

                var matchingElements = doc.Elements(nodeNameHierarchy.First()).ToList();
                if (matchingElements.Count == 0)
                {
                    OnErrorEvent(string.Format("PluginInfo file is missing node {0}: {1}", nodeNameHierarchy.First(), pluginInfoFile.FullName));
                    return false;
                }

                foreach (var nodeName in nodeNameHierarchy.Skip(1))
                {
                    matchingElements = matchingElements.First().Elements(nodeName).ToList();
                    if (matchingElements.Count == 0)
                    {
                        OnErrorEvent(string.Format("PluginInfo file is missing node {0}: {1}", nodeName, pluginInfoFile.FullName));
                        return false;
                    }
                }

                if (GetToolInfo(pluginInfoFile, toolName, matchingElements, out className, out assemblyName))
                    return true;

                if (toolName.StartsWith("test_"))
                {
                    var alternateName = toolName.Substring(5);

                    OnWarningEvent(string.Format("Could not resolve tool name '{0}'; will try '{1}'", toolName, alternateName));

                    if (GetToolInfo(pluginInfoFile, alternateName, matchingElements, out className, out assemblyName))
                        return true;
                }

                OnErrorEvent(string.Format(
                    "Could not resolve {0} tool name {1} in {2}", classType, toolName, pluginInfoFile.FullName));

                return false;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in GetPluginInfo reading " + pluginInfoFilePath, ex);
                return false;
            }
        }

        private bool GetToolInfo(
            FileInfo pluginInfoFile,
            string toolName,
            IEnumerable<XElement> matchingElements,
            out string className,
            out string assemblyName)
        {
            foreach (var element in matchingElements)
            {
                if (!element.HasAttributes)
                    continue;

                if (!TryGetAttribute(pluginInfoFile, element, "Tool", out var candidateToolName))
                    continue;

                if (!candidateToolName.Equals(toolName, StringComparison.OrdinalIgnoreCase))
                    continue;

                if (!TryGetAttribute(pluginInfoFile, element, "Class", out className))
                    continue;

                if (!TryGetAttribute(pluginInfoFile, element, "AssemblyFile", out assemblyName))
                    continue;

                return true;
            }

            className = string.Empty;
            assemblyName = string.Empty;

            return false;
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
                        OnErrorEvent("Plugin info file not found: " + pluginInfoFilePath);
                        return null;
                    }
                }

                if (TraceMode)
                    OnDebugEvent("Call System.Reflection.Assembly.LoadFrom for assembly " + PathUtils.CompactPathString(pluginInfoFile.FullName, 60));

                var assembly = System.Reflection.Assembly.LoadFrom(pluginInfoFile.FullName);

                var assemblyType = assembly.GetType(className, false, true);

                if (assemblyType == null)
                    throw new Exception(string.Format(
                        "assembly.GetType returned null for class {0}; " +
                        "examine plugin_info.xml for the mapping from step tool name to assembly and class", className));

                var instance = Activator.CreateInstance(assemblyType);
                return instance;
            }
            catch (Exception ex)
            {
                // Cache exceptions
                OnErrorEvent(string.Format("PluginLoader.LoadObject(), for class {0}, assembly {1}", className, assemblyName), ex);
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
            var nodeNameHierarchy = new List<string>
            {
                "Plugins",
                "ToolRunners",
                "ToolRunner"
            };

            if (!GetPluginInfo("ToolRunner", toolName, nodeNameHierarchy, out var className, out var assemblyName))
            {
                mSummaryFile.Add("Unable to load ToolRunner for " + toolName);
                return null;
            }

#if PLUGIN_DEBUG_MODE_ENABLED
            // This constant is defined on the Build tab of the Analysis Manager solution
            var debugToolRunner = DebugModeGetToolRunner(className);

            if (debugToolRunner != null)
            {
                return debugToolRunner;
            }
#endif

            var newToolRunner = LoadObject(className, assemblyName);
            if (newToolRunner == null)
                return null;

            try
            {
                var myToolRunner = (IToolRunner)newToolRunner;
                mSummaryFile.Add("Loaded ToolRunner: " + className + " from " + assemblyName);
                return myToolRunner;
            }
            catch (Exception ex)
            {
                OnErrorEvent(string.Format("PluginLoader.GetToolRunner(), for class {0}, assembly {1}", className, assemblyName), ex);
                return null;
            }
        }

        /// <summary>
        /// Loads a resourcer object
        /// </summary>
        /// <param name="toolName">Name of analysis tool</param>
        /// <returns>An object meeting the IAnalysisResources interface</returns>
        public IAnalysisResources GetAnalysisResources(string toolName)
        {
            var nodeNameHierarchy = new List<string>
            {
                "Plugins",
                "Resourcers",
                "Resourcer"
            };

            if (!GetPluginInfo("Resourcer", toolName, nodeNameHierarchy, out var className, out var assemblyName))
            {
                mSummaryFile.Add("Unable to load Resourcer for " + toolName);
                return null;
            }

#if PLUGIN_DEBUG_MODE_ENABLED
            // This constant is defined on the Build tab of the Analysis Manager solution
            var debugResourcer = DebugModeGetAnalysisResources(className);
            if (debugResourcer != null)
            {
                return debugResourcer;
            }
#endif

            var newResourcer = LoadObject(className, assemblyName);
            if (newResourcer == null)
                return null;

            try
            {
                var myModule = (IAnalysisResources)newResourcer;
                mSummaryFile.Add("Loaded resourcer: " + className + " from " + assemblyName);
                return myModule;
            }
            catch (Exception ex)
            {
                OnErrorEvent(string.Format("PluginLoader.GetAnalysisResources(), for class {0}, assembly {1}", className, assemblyName), ex);
                return null;
            }
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

        private bool TryGetAttribute(FileSystemInfo pluginInfoFile, XElement element, string attributeName, out string attributeValue)
        {
            if (Global.TryGetAttribute(element, attributeName, out attributeValue))
                return true;

            OnWarningEvent(string.Format("Attribute {0} not found for the current element in {1}", attributeName, pluginInfoFile.FullName));
            return false;
        }
    }

    #endregion
}