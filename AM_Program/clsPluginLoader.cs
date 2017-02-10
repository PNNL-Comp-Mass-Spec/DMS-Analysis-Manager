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
                //case "analysismanagerxtandemplugin.clsanalysistoolrunnerxt":
                //    myToolRunner = new AnalysisManagerXTandemPlugIn.clsAnalysisToolRunnerXT();
                //    break;
                //case "analysismanagerdtasplitplugin.clsanalysistoolrunnerdtasplit":
                //    myToolRunner = new AnalysisManagerDtaSplitPlugIn.clsAnalysisToolRunnerDtaSplit();
                //    break;
                //case "analysismanagerinspectplugin.clsanalysistoolrunnerin":
                //    myToolRunner = new AnalysisManagerInSpecTPlugIn.clsAnalysisToolRunnerIN();
                //    break;
                //case "analysismanagerinspresultsassemblyplugin.clsanalysistoolrunnerinspresultsassembly":
                //    myToolRunner = new AnalysisManagerInspResultsAssemblyPlugIn.clsAnalysisToolRunnerInspResultsAssembly();
                //    break;
                //case "analysismanagerdecon2lsplugin.clsanalysistoolrunnerdecon2lsdeisotope":
                //    myToolRunner = new AnalysisManagerDecon2lsPlugIn.clsAnalysisToolRunnerDecon2lsDeIsotope();
                //    break;
                //case "analysismanagerdecon2lsplugin.clsanalysistoolrunnerdecon2lstic":
                //    myToolRunner = new AnalysisManagerDecon2lsPlugIn.clsAnalysisToolRunnerDecon2lsDeIsotope();
                //    break;
                //case "analysismanagermsclusterdtatodatplugin.clsanalysistoolrunnerdtatodat":
                //    myToolRunner = new AnalysisManagerMSClusterDTAtoDATPlugIn.clsAnalysisToolRunnerDTAtoDAT();
                //    break;
                //case "analysismanagerextractionplugin.clsextracttoolrunner":
                //    myToolRunner = new AnalysisManagerExtractionPlugin.clsExtractToolRunner();
                //    break;
                //case "analysismanagermsxmlgenplugin.clsanalysistoolrunnermsxmlgen":
                //    myToolRunner = new AnalysisManagerMsXmlGenPlugIn.clsAnalysisToolRunnerMSXMLGen();
                //    break;
                //case "analysismanagermsxmlbrukerplugin.clsanalysistoolrunnermsxmlbruker":
                //    myToolRunner = new AnalysisManagerMsXmlBrukerPlugIn.clsAnalysisToolRunnerMSXMLBruker();
                //    break;
                //case "msmsspectrumfilteram.clsanalysistoolrunnermsmsspectrumfilter":
                //    myToolRunner = new MSMSSpectrumFilterAM.clsAnalysisToolRunnerMsMsSpectrumFilter();
                //    break;
                //case "analysismanagermasicplugin.clsanalysistoolrunnermasicfinnigan":
                //    myToolRunner = new AnalysisManagerMasicPlugin.clsAnalysisToolRunnerMASICFinnigan();
                //    break;
                //case "analysismanagericr2lsplugin.clsanalysistoolrunnericr":
                //    myToolRunner = new AnalysisManagerICR2LSPlugIn.clsAnalysisToolRunnerICR();
                //    break;
                //case "analysismanagericr2lsplugin.clsanalysistoolrunnerltq_ftpek":
                //    myToolRunner = new AnalysisManagerICR2LSPlugIn.clsAnalysisToolRunnerLTQ_FTPek();
                //    break;
                //case "analysismanagerlcmsfeaturefinderplugin.clsanalysistoolrunnerlcmsff":
                //    myToolRunner = new AnalysisManagerLCMSFeatureFinderPlugIn.clsAnalysisToolRunnerLCMSFF();
                //    break;
                //case "analysismanageromssaplugin.clsanalysistoolrunnerom":
                //    myToolRunner = new AnalysisManagerOMSSAPlugin.clsAnalysisToolRunnerOM();
                //    break;
                //case "analysismanagerdecon2lsv2plugin.clsanalysistoolrunnerdecon2ls":
                //    myToolRunner = new AnalysisManagerDecon2lsV2PlugIn.clsAnalysisToolRunnerDecon2ls();
                //    break;
                //case "analysismanagerdtarefineryplugin.clsanalysistoolrunnerdtarefinery":
                //    myToolRunner = new AnalysisManagerDtaRefineryPlugIn.clsAnalysisToolRunnerDtaRefinery();
                //    break;
                //case "analysismanagerpridemzxmlplugin.clsanalysistoolrunnerpridemzxml":
                //    myToolRunner = new AnalysisManagerPRIDEMzXMLPlugIn.clsAnalysisToolRunnerPRIDEMzXML();
                //    break;
                //case "analysismanagerphospho_fdr_aggregatorplugin.clsanalysistoolrunnerphosphofdraggregator":
                //    myToolRunner = new AnalysisManagerPhospho_FDR_AggregatorPlugIn.clsAnalysisToolRunnerPhosphoFdrAggregator();
                //    break;
                //case "analysismanagermsgfplugin.clsmsgfrunner":
                //    myToolRunner = new AnalysisManagerMSGFPlugin.clsMSGFRunner();
                //    break;
                //case "analysismanagermsgfdbplugin.clsanalysistoolrunnermsgfdb":
                //    myToolRunner = new AnalysisManagerMSGFDBPlugIn.clsAnalysisToolRunnerMSGFDB();
                //    break;
                //case "analysismanagermsdeconvplugin.clsanalysistoolrunnermsdeconv":
                //    myToolRunner = new AnalysisManagerMSDeconvPlugIn.clsAnalysisToolRunnerMSDeconv();
                //    break;
                //case "analysismanagermsalignplugin.clsanalysistoolrunnermsalign":
                //    myToolRunner = new AnalysisManagerMSAlignPlugIn.clsAnalysisToolRunnerMSAlign();
                //    break;
                //case "analysismanagermsalignplugin.clsanalysistoolrunnermsalignhistone":
                //    myToolRunner = new AnalysisManagerMSAlignHistonePlugIn.clsAnalysisToolRunnerMSAlignHistone();
                //    break;
                //case "analysismanagersmaqcplugin.clsanalysistoolrunnersmaqc":
                //    myToolRunner = new AnalysisManagerSMAQCPlugIn.clsAnalysisToolRunnerSMAQC();
                //    break;
                //case "dtaspectrafilegen.clsdtagentoolrunner":
                //    myToolRunner = new DTASpectraFileGen.clsDtaGenToolRunner();
                //    break;
                //case "analysismanagersequestplugin.clsanalysistoolrunnerseqcluster":
                //    myToolRunner = new AnalysisManagerSequestPlugin.clsAnalysisToolRunnerSeqCluster();
                //    break;
                //case "analysismanageridpickerplugin.clsanalysistoolrunneridpicker":
                //    myToolRunner = new AnalysisManagerIDPickerPlugIn.clsAnalysisToolRunnerIDPicker();
                //    break;
                //case "analysismanagerlipidmapsearchplugin.clsanalysistoolrunnerlipidmapsearch":
                //    myToolRunner = new AnalysisManagerLipidMapSearchPlugIn.clsAnalysisToolRunnerLipidMapSearch();
                //    break;
                //case "analysismanagermsalignquantplugin.clsanalysistoolrunnermsalignquant":
                //    myToolRunner = new AnalysisManagerMSAlignQuantPlugIn.clsAnalysisToolRunnerMSAlignQuant();
                //    break;
                //case "analysismanagerprideconverterplugin.clsanalysistoolrunnerprideconverter":
                //    myToolRunner = new AnalysisManagerPRIDEConverterPlugIn.clsAnalysisToolRunnerPRIDEConverter();
                //    break;
                //case "analysismanagermultialign_aggregatorplugin.clsanalysistoolrunnermultialignaggregator":
                //    myToolRunner = new AnalysisManagerMultiAlign_AggregatorPlugIn.clsAnalysisToolRunnerMultiAlignAggregator();
                //    break;
                //case "analysismanager_mage_plugin.clsanalysistoolrunnermage":
                //    myToolRunner = new AnalysisManager_Mage_PlugIn.clsAnalysisToolRunnerMage();
                //    break;
                //case "analysismanager_cyclops_plugin.clsanalysistoolrunnercyclops":
                //    myToolRunner = new AnalysisManager_Cyclops_PlugIn.clsAnalysisToolRunnerCyclops();
                //    break;
                //case "analysismanager_ascore_plugin.clsanalysistoolrunnerascore":
                //    myToolRunner = new AnalysisManager_AScore_PlugIn.clsAnalysisToolRunnerAScore();
                //    break;
                //case "analysismanager_ape_plugin.clsanalysistoolrunnerape":
                //    myToolRunner = new AnalysisManager_Ape_PlugIn.clsAnalysisToolRunnerApe();
                //    break;
                //case "analysismanager_repopkgr_plugin.clsanalysistoolrunnerrepopkgr":
                //    myToolRunner = new AnalysisManager_RepoPkgr_Plugin.clsAnalysisToolRunnerRepoPkgr();
                //    break;
                //case "analysismanagerresultsxferplugin.clsresultxfertoolrunner":
                //    myToolRunner = new AnalysisManagerResultsXferPlugin.clsResultXferToolRunner();
                //    break;
                //case "analysismanagermodaplugin.clsanalysistoolrunnermoda":
                //    myToolRunner = new AnalysisManagerMODaPlugIn.clsAnalysisToolRunnerMODa();
                //    break;
                //case "analysismanagerdeconpeakdetectorplugin.clsanalysistoolrunnerdeconpeakdetector":
                //    myToolRunner = new AnalysisManagerDeconPeakDetectorPlugIn.clsAnalysisToolRunnerDeconPeakDetector();
                //    break;
                //case "analysismanagerglyqiqplugin.clsanalysistoolrunnerglyqiq":
                //    myToolRunner = new AnalysisManagerGlyQIQPlugIn.clsAnalysisToolRunnerGlyQIQ();
                //    break;
                //case "analysismanagernomsiplugin.clsanalysistoolrunnernomsi":
                //    myToolRunner = new AnalysisManagerNOMSIPlugin.clsAnalysisToolRunnerNOMSI();
                //    break;
                //case "analysismanagerqcartplugin.clsanalysistoolrunnerqcart":
                //    myToolRunner = new AnalysisManagerQCARTPlugin.clsAnalysisToolRunnerQCART();
                //    break;
            }

            return myToolRunner;
        }

        private IAnalysisResources DebugModeGetAnalysisResources(string className)
        {
            IAnalysisResources myModule = null;

            switch (className.ToLower())
            {
                //case "analysismanagerxtandemplugin.clsanalysisresourcesxt":
                //    myModule = new AnalysisManagerXTandemPlugIn.clsAnalysisResourcesXT();
                //    break;
                //case "analysismanagerdtasplitplugin.clsanalysisresourcesdtasplit":
                //    myModule = new AnalysisManagerDtaSplitPlugIn.clsAnalysisResourcesDtaSplit();
                //    break;
                //case "analysismanagerinspectplugin.clsanalysisresourcesin":
                //    myModule = new AnalysisManagerInSpecTPlugIn.clsAnalysisResourcesIN();
                //    break;
                //case "analysismanagerinspresultsassemblyplugin.clsanalysisresourcesinspresultsassembly":
                //    myModule = new AnalysisManagerInspResultsAssemblyPlugIn.clsAnalysisResourcesInspResultsAssembly();
                //    break;
                //case "analysismanagerdecon2lsplugin.clsanalysisresourcesdecon2ls":
                //    myModule = new AnalysisManagerDecon2lsPlugIn.clsAnalysisResourcesDecon2ls();
                //    break;
                //case "analysismanagermsclusterdtatodatplugin.clsanalysisresourcesdtatodat":
                //    myModule = new AnalysisManagerMSClusterDTAtoDATPlugIn.clsAnalysisResourcesDTAtoDAT();
                //    break;
                //case "analysismanagerextractionplugin.clsanalysisresourcesextraction":
                //    myModule = new AnalysisManagerExtractionPlugin.clsAnalysisResourcesExtraction();
                //    break;
                //case "analysismanagermsxmlgenplugin.clsanalysisresourcesmsxmlgen":
                //    myModule = new AnalysisManagerMsXmlGenPlugIn.clsAnalysisResourcesMSXMLGen();
                //    break;
                //case "analysismanagermsxmlbrukerplugin.clsanalysisresourcesmsxmlbruker":
                //    myModule = new AnalysisManagerMsXmlBrukerPlugIn.clsAnalysisResourcesMSXMLBruker();
                //    break;
                //case "msmsspectrumfilteram.clsanalysisresourcesmsmsspectrumfilter":
                //    myModule = new MSMSSpectrumFilterAM.clsAnalysisResourcesMsMsSpectrumFilter();
                //    break;
                //case "analysismanagermasicplugin.clsanalysisresourcesmasic":
                //    myModule = new AnalysisManagerMasicPlugin.clsAnalysisResourcesMASIC();
                //    break;
                //case "analysismanagericr2lsplugin.clsanalysisresourcesicr2ls":
                //    myModule = new AnalysisManagerICR2LSPlugIn.clsAnalysisResourcesIcr2ls();
                //    break;
                //case "analysismanagericr2lsplugin.clsanalysisresourcesltq_ftpek":
                //    myModule = new AnalysisManagerICR2LSPlugIn.clsAnalysisResourcesLTQ_FTPek();
                //    break;
                //case "analysismanagerlcmsfeaturefinderplugin.clsanalysisresourceslcmsff":
                //    myModule = new AnalysisManagerLCMSFeatureFinderPlugIn.clsAnalysisResourcesLCMSFF();
                //    break;
                //case "analysismanageromssaplugin.clsanalysisresourcesom":
                //    myModule = new AnalysisManagerOMSSAPlugin.clsAnalysisResourcesOM();
                //    break;
                //case "analysismanagerdecon2lsv2plugin.clsanalysisresourcesdecon2ls":
                //    myModule = new AnalysisManagerDecon2lsV2PlugIn.clsAnalysisResourcesDecon2ls();
                //    break;
                //case "analysismanagerdtarefineryplugin.clsanalysisresourcesdtarefinery":
                //    myModule = new AnalysisManagerDtaRefineryPlugIn.clsAnalysisResourcesDtaRefinery();
                //    break;
                //case "analysismanagerpridemzxmlplugin.clsanalysisresourcespridemzxml":
                //    myModule = new AnalysisManagerPRIDEMzXMLPlugIn.clsAnalysisResourcesPRIDEMzXML();
                //    break;
                //case "analysismanagerphospho_fdr_aggregatorplugin.clsanalysisresourcesphosphofdraggregator":
                //    myModule = new AnalysisManagerPhospho_FDR_AggregatorPlugIn.clsAnalysisResourcesPhosphoFdrAggregator();
                //    break;
                //case "analysismanagermsgfplugin.clsanalysisresourcesmsgf":
                //    myModule = new AnalysisManagerMSGFPlugin.clsAnalysisResourcesMSGF();
                //    break;
                //case "analysismanagermsgfdbplugin.clsanalysisresourcesmsgfdb":
                //    myModule = new AnalysisManagerMSGFDBPlugIn.clsAnalysisResourcesMSGFDB();
                //    break;
                //case "analysismanagermsdeconvplugin.clsanalysisresourcesmsdeconv":
                //    myModule = new AnalysisManagerMSDeconvPlugIn.clsAnalysisResourcesMSDeconv();
                //    break;
                //case "analysismanagermsalignplugin.clsanalysisresourcesmsalign":
                //    myModule = new AnalysisManagerMSAlignPlugIn.clsAnalysisResourcesMSAlign();
                //    break;
                //case "analysismanagermsalignhistoneplugin.clsanalysisresourcesmsalignhistone":
                //    myModule = new AnalysisManagerMSAlignHistonePlugIn.clsAnalysisResourcesMSAlignHistone();
                //    break;
                //case "analysismanagersmaqcplugin.clsanalysisresourcessmaqc":
                //    myModule = new AnalysisManagerSMAQCPlugIn.clsAnalysisResourcesSMAQC();
                //    break;
                //case "dtaspectrafilegen.clsdtagenresources":
                //    myModule = new DTASpectraFileGen.clsDtaGenResources();
                //    break;
                //case "analysismanagersequestplugin.clsanalysisresourcesseq":
                //    myModule = new AnalysisManagerSequestPlugin.clsAnalysisResourcesSeq();
                //    break;
                //case "analysismanageridpickerplugin.clsanalysisresourcesidpicker":
                //    myModule = new AnalysisManagerIDPickerPlugIn.clsAnalysisResourcesIDPicker();
                //    break;
                //case "analysismanagerlipidmapsearchplugin.clsanalysisresourceslipidmapsearch":
                //    myModule = new AnalysisManagerLipidMapSearchPlugIn.clsAnalysisResourcesLipidMapSearch();
                //    break;
                //case "analysismanagermsalignquantplugin.clsanalysisresourcesmsalignquant":
                //    myModule = new AnalysisManagerMSAlignQuantPlugIn.clsAnalysisResourcesMSAlignQuant();
                //    break;
                //case "analysismanagerprideconverterplugin.clsanalysisresourcesprideconverter":
                //    myModule = new AnalysisManagerPRIDEConverterPlugIn.clsAnalysisResourcesPRIDEConverter();
                //    break;
                //case "analysismanagermultialign_aggregatorplugin.clsanalysisresourcesmultialignaggregator":
                //    myModule = new AnalysisManagerMultiAlign_AggregatorPlugIn.clsAnalysisResourcesMultiAlignAggregator();
                //    break;
                //case "analysismanager_mage_plugin.clsanalysisresourcesmage":
                //    myModule = new AnalysisManager_Mage_PlugIn.clsAnalysisResourcesMage();
                //    break;
                //case "analysismanager_cyclops_plugin.clsanalysisresourcescyclops":
                //    myModule = new AnalysisManager_Cyclops_PlugIn.clsAnalysisResourcesCyclops();
                //    break;
                //case "analysismanager_ascore_plugin.clsanalysisresourcesascore":
                //    myModule = new AnalysisManager_AScore_PlugIn.clsAnalysisResourcesAScore();
                //    break;
                //case "analysismanager_ape_plugin.clsanalysisresourcesape":
                //    myModule = new AnalysisManager_Ape_PlugIn.clsAnalysisResourcesApe();
                //    break;
                //case "analysismanager_repopkgr_plugin.clsanalysisresourcesrepopkgr":
                //    myModule = new AnalysisManager_RepoPkgr_Plugin.clsAnalysisResourcesRepoPkgr();
                //    break;
                //case "analysismanagerresultsxferplugin.clsanalysisresourcesresultxfer":
                //    myModule = new AnalysisManagerResultsXferPlugin.clsAnalysisResourcesResultXfer();
                //    break;
                //case "analysismanagermodaplugin.clsanalysisresourcesmoda":
                //    myModule = new AnalysisManagerMODaPlugIn.clsAnalysisResourcesMODa();
                //    break;
                //case "analysismanagerdeconpeakdetectorplugin.clsanalysisresourcesdeconpeakdetector":
                //    myModule = new AnalysisManagerDeconPeakDetectorPlugIn.clsAnalysisResourcesDeconPeakDetector();
                //    break;
                //case "analysismanagerglyqiqplugin.clsanalysisresourcesglyqiq":
                //    myModule = new AnalysisManagerGlyQIQPlugIn.clsAnalysisResourcesGlyQIQ();
                //    break;
                //case "analysismanagerqcartplugin.clsanalysisresourcesqcart":
                //    myModule = new AnalysisManagerQCARTPlugin.clsAnalysisResourcesQCART();
                //    break;
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
                XmlElement root = doc.DocumentElement;

                //find the element that matches the tool name
                var nodeList = root.SelectNodes(XPath);

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
                Type t = a.GetType(className, false, true);
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
