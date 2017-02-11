//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2007, Battelle Memorial Institute
// Created 12/19/2007
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Xml;
using AnalysisManagerBase;

namespace AnalysisManagerProg
{
    /// <summary>
    /// Class for loading analysis manager plugins
    /// </summary>
    public class clsPluginLoader
    {
        #region "Member variables"

        private readonly List<string> m_ErrorMessages = new List<string>();
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
            get { return m_pluginConfigFile; }
            set { m_pluginConfigFile = value; }
        }

        /// <summary>
        /// Exceptions that occur in the call to GetAnalysisResources or GetToolRunner
        /// </summary>
        public List<string> ErrorMessages => m_ErrorMessages;

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

        /// <summary>
        /// Clears internal list of error messages
        /// </summary>
        /// <remarks></remarks>
        public void ClearMessageList()
        {
            // Clears the message list
            m_ErrorMessages.Clear();
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
            var strPluginInfo = string.Empty;

            className = string.Empty;
            assyName = string.Empty;

            try
            {
                if (string.IsNullOrEmpty(XPath))
                {
                    throw new ArgumentException("XPath must be defined", nameof(XPath));
                }

                strPluginInfo = "XPath=\"" + XPath + "\"; className=\"" + className + "\"; assyName=" + assyName + "\"";

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
                    throw new Exception(string.Format("XPath did not have a match for '{0}' in {1}", strPluginInfo, m_pluginConfigFile));
                }

                // make sure that we found exactly one element,
                // and if we did, retrieve its information
                if (nodeList.Count != 1)
                {
                    throw new Exception("Could not resolve tool name; " + strPluginInfo);
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
                m_ErrorMessages.Add("Error in GetPluginInfo:" + ex.Message + "; " + strPluginInfo);
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
            object obj = null;
            m_ErrorMessages.Clear();

            try
            {
                //Build instance of tool runner subclass from class name and assembly file name.
                var a = System.Reflection.Assembly.LoadFrom(GetPluginInfoFilePath(assyName));
                var t = a.GetType(className, false, true);
                obj = Activator.CreateInstance(t);
            }
            catch (Exception ex)
            {
                // Cache exceptions
                m_ErrorMessages.Add("clsPluginLoader.LoadObject(), exception: " + ex.Message);
            }
            return obj;
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

            string className;
            string assyName;
            IToolRunner myToolRunner = null;

            if (GetPluginInfo(xpath, out className, out assyName))
            {
#if PLUGIN_DEBUG_MODE_ENABLED
                myToolRunner = DebugModeGetToolRunner(className);
                if (myToolRunner != null)
                {
                    return myToolRunner;
                }
#endif

                var obj = LoadObject(className, assyName);
                if (obj != null)
                {
                    try
                    {
                        myToolRunner = (IToolRunner)obj;
                    }
                    catch (Exception ex)
                    {
                        // Cache exceptions
                        m_ErrorMessages.Add(ex.Message);
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
            string className;
            string assyName;
            IAnalysisResources myModule = null;

            if (GetPluginInfo(xpath, out className, out assyName))
            {
#if PLUGIN_DEBUG_MODE_ENABLED
                myModule = DebugModeGetAnalysisResources(className);
                if ((myModule != null))
                {
                    return myModule;
                }
#endif

                var obj = LoadObject(className, assyName);
                if ((obj != null))
                {
                    try
                    {
                        myModule = (IAnalysisResources)obj;
                    }
                    catch (Exception ex)
                    {
                        // Cache exceptions
                        m_ErrorMessages.Add(ex.Message);
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
