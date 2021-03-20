// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Design", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "Ignore errors here", Scope = "member", Target = "~M:AnalysisManagerMODPlusPlugin.MODPlusResultsReader.ReadNextSpectrum~System.Boolean")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not needed", Scope = "member", Target = "~M:AnalysisManagerMODPlusPlugin.MODPlusResultsReader.ReadNextSpectrum~System.Boolean")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable legacy name", Scope = "type", Target = "~T:AnalysisManagerMODPlusPlugin.AnalysisToolRunnerMODPlus")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable legacy name", Scope = "type", Target = "~T:AnalysisManagerMODPlusPlugin.MODPlusResultsReader")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable legacy name", Scope = "type", Target = "~T:AnalysisManagerMODPlusPlugin.MODPlusRunner")]
[assembly: SuppressMessage("Usage", "RCS1146:Use conditional access.", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:AnalysisManagerMODPlusPlugin.AnalysisToolRunnerMODPlus.CreateThreadParamFiles(System.IO.FileInfo,System.Xml.XmlNode,System.Collections.Generic.IEnumerable{System.IO.FileInfo})~System.Collections.Generic.Dictionary{System.Int32,System.String}")]
[assembly: SuppressMessage("Usage", "RCS1146:Use conditional access.", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:AnalysisManagerMODPlusPlugin.AnalysisToolRunnerMODPlus.DefineParamfileDatasetAndFasta(System.Xml.XmlDocument,System.String)")]
[assembly: SuppressMessage("Usage", "RCS1146:Use conditional access.", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:AnalysisManagerMODPlusPlugin.AnalysisToolRunnerMODPlus.DefineParamMassResolutionSettings(System.Xml.XmlDocument)")]
[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "member", Target = "~M:AnalysisManagerMODPlusPlugin.AnalysisToolRunnerMODPlus.MakeXPath(System.Xml.XmlDocument,System.Xml.XmlNode,System.String,System.Collections.Generic.Dictionary{System.String,System.String})~System.Xml.XmlNode")]
