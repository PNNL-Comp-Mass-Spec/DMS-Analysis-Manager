//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2007, Battelle Memorial Institute
// Created 12/19/2007
//
//*********************************************************************************************************

using System;
using System.IO;
using System.Xml;
using AnalysisManagerBase;
using PRISM;

namespace AnalysisManagerProg
{
    /// <summary>
    /// Class for loading analysis manager plugins
    /// </summary>
    public class clsPluginLoader : clsEventNotifier
    {
        #region "Member variables"

        private readonly string m_MgrFolderPath;
        private string m_pluginConfigFile = "plugin_info.xml";
        private readonly clsSummaryFile m_SummaryFile;

        #endregion

        #region "Properties"

        /// <summary>
        /// Plugin config file
        /// </summary>
        /// <remarks>Defaults to plugin_info.xml</remarks>
        public string FileName
        {
            get => m_pluginConfigFile;
            set => m_pluginConfigFile = value;
        }

        /// <summary>
        /// When true, show additional messages at the console
        /// </summary>
        public bool TraceMode { get; set; }

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="objSummaryFile"></param>
        /// <param name="MgrFolderPath"></param>
        public clsPluginLoader(clsSummaryFile objSummaryFile, string MgrFolderPath)
        {
            m_SummaryFile = objSummaryFile;
            m_MgrFolderPath = MgrFolderPath;
        }

#if PLUGIN_DEBUG_MODE_ENABLED

        private IToolRunner DebugModeGetToolRunner(string className)
        {
            IToolRunner myToolRunner = null;

            switch (className.ToLower())
            {
                case "analysismanagermsgfdbplugin.clsanalysistoolrunnermsgfdb":
                //    myToolRunner = new AnalysisManagerMSGFDBPlugIn.clsAnalysisToolRunnerMSGFDB();
                    break;
                case "analysismanagerqcartplugin.clsanalysistoolrunnerqcart":
                //    myToolRunner = new AnalysisManagerQCARTPlugin.clsAnalysisToolRunnerQCART();
                    break;
            }

            return myToolRunner;
        }

        private IAnalysisResources DebugModeGetAnalysisResources(string className)
        {
            IAnalysisResources myModule = null;

            switch (className.ToLower())
            {
                case "analysismanagermsgfdbplugin.clsanalysisresourcesmsgfdb":
                //    myModule = new AnalysisManagerMSGFDBPlugIn.clsAnalysisResourcesMSGFDB();
                    break;
                case "analysismanagerqcartplugin.clsanalysisresourcesqcart":
                //    myModule = new AnalysisManagerQCARTPlugin.clsAnalysisResourcesQCART();
                    break;
            }

            return myModule;
        }
#endif

        /// <summary>
        /// Retrieves data for specified plugin from plugin info config file
        /// </summary>
        /// <param name="XPath">XPath spec for specified plugin</param>
        /// <param name="className">Name of class for plugin (return value) </param>
        /// <param name="assyName">Name of assembly for plugin (return value)</param>
        /// <returns>TRUE for success, FALSE for failure</returns>
        /// <remarks></remarks>
        private bool GetPluginInfo(string XPath, out string className, out string assyName)
        {
            var doc = new XmlDocument();
            var pluginInfo = string.Empty;

            className = string.Empty;
            assyName = string.Empty;

            try
            {
                if (string.IsNullOrEmpty(XPath))
                {
                    throw new ArgumentException("XPath must be defined", nameof(XPath));
                }

                pluginInfo = "XPath=\"" + XPath + "\"; className=\"" + className + "\"; assyName=" + assyName + "\"";

                //read the tool runner info file
                doc.Load(GetPluginInfoFilePath(m_pluginConfigFile));
                var root = doc.DocumentElement;

                if (root == null)
                {
                    throw new Exception("Valid XML not found in file " + m_pluginConfigFile);
                }

                // find the element that matches the tool name
                var nodeList = root.SelectNodes(XPath);

                if (nodeList == null)
                {
                    throw new Exception(string.Format("XPath did not have a match for '{0}' in {1}", pluginInfo, m_pluginConfigFile));
                }

                // make sure that we found exactly one element,
                // and if we did, retrieve its information
                if (nodeList.Count != 1)
                {
                    throw new Exception("Could not resolve tool name; " + pluginInfo);
                }

                foreach (XmlElement n in nodeList)
                {
                    className = n.GetAttribute("Class");
                    assyName = n.GetAttribute("AssemblyFile");
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
        /// Loads the specifed dll
        /// </summary>
        /// <param name="className">Name of class to load (from GetPluginInfo)</param>
        /// <param name="assyName">Name of assembly to load (from GetPluginInfo)</param>
        /// <returns>An object referencing the specified dll</returns>
        /// <remarks></remarks>
        private object LoadObject(string className, string assyName)
        {

            try
            {
                // Build instance of tool runner subclass from class name and assembly file name.
                var pluginInfoFilePath = GetPluginInfoFilePath(assyName);

                // Make sure the file exists and is capitalized correctly
                var pluginInfoFile = new FileInfo(pluginInfoFilePath);
                var expectedName = Path.GetFileName(pluginInfoFilePath);

                if (!pluginInfoFile.Exists)
                {
                    var pluginFolder = new DirectoryInfo(m_MgrFolderPath);
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
                    OnDebugEvent("Call System.Reflection.Assembly.LoadFrom for assembly " + pluginInfoFile.FullName);

                var assembly = System.Reflection.Assembly.LoadFrom(pluginInfoFile.FullName);

                var assemblyType = assembly.GetType(className, false, true);

                var instance = Activator.CreateInstance(assemblyType);
                return instance;
            }
            catch (Exception ex)
            {
                // Cache exceptions
                OnErrorEvent(string.Format("clsPluginLoader.LoadObject(), for class {0}, assembly {1}", className, assyName), ex);
                return null;
            }

        }

        /// <summary>
        /// Loads a tool runner object
        /// </summary>
        /// <param name="toolName">Name of tool</param>
        /// <returns>An object meeting the IToolRunner interface</returns>
        /// <remarks></remarks>
        public IToolRunner GetToolRunner(string toolName)
        {
            var xpath = "//ToolRunners/ToolRunner[@Tool='" + toolName.ToLower() + "']";

            IToolRunner myToolRunner = null;

            if (GetPluginInfo(xpath, out var className, out var assyName))
            {
#if PLUGIN_DEBUG_MODE_ENABLED
                myToolRunner = DebugModeGetToolRunner(className);
                if (myToolRunner != null)
                {
                    return myToolRunner;
                }
#endif

                var newToolRunner = LoadObject(className, assyName);
                if (newToolRunner != null)
                {
                    try
                    {
                        myToolRunner = (IToolRunner)newToolRunner;
                    }
                    catch (Exception ex)
                    {
                        OnErrorEvent(string.Format("clsPluginLoader.GetToolRunner(), for class {0}, assembly {1}", className, assyName), ex);
                    }
                }
            }
            m_SummaryFile.Add("Loaded ToolRunner: " + className + " from " + assyName);
            return myToolRunner;
        }

        /// <summary>
        /// Loads a resourcer object
        /// </summary>
        /// <param name="toolName">Name of analysis tool</param>
        /// <returns>An object meeting the IAnalysisResources interface</returns>
        /// <remarks></remarks>
        public IAnalysisResources GetAnalysisResources(string toolName)
        {
            var xpath = "//Resourcers/Resourcer[@Tool='" + toolName + "']";
            IAnalysisResources myModule = null;

            if (GetPluginInfo(xpath, out var className, out var assyName))
            {
#if PLUGIN_DEBUG_MODE_ENABLED
                myModule = DebugModeGetAnalysisResources(className);
                if (myModule != null)
                {
                    return myModule;
                }
#endif

                var newResourcer = LoadObject(className, assyName);
                if (newResourcer != null)
                {
                    try
                    {
                        myModule = (IAnalysisResources)newResourcer;
                    }
                    catch (Exception ex)
                    {
                        OnErrorEvent(string.Format("clsPluginLoader.GetAnalysisResources(), for class {0}, assembly {1}", className, assyName), ex);
                    }
                }
                m_SummaryFile.Add("Loaded resourcer: " + className + " from " + assyName);
            }
            else
            {
                m_SummaryFile.Add("Unable to load resourcer for tool " + toolName);
            }
            return myModule;
        }

        /// <summary>
        /// Gets the path to the plugin info config file
        /// </summary>
        /// <param name="pluginInfoFileName">Name of plugin info file</param>
        /// <returns>Path to plugin info file</returns>
        /// <remarks></remarks>
        private string GetPluginInfoFilePath(string pluginInfoFileName)
        {
            return Path.Combine(m_MgrFolderPath, pluginInfoFileName);
        }
        #endregion
    }
}
