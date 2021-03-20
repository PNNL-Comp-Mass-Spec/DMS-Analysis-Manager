// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Design", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "Ignore errors here", Scope = "member", Target = "~M:AnalysisManagerPhospho_FDR_AggregatorPlugIn.AnalysisToolRunnerPhosphoFdrAggregator.ProcessSynopsisFiles(System.String,System.Collections.Generic.List{System.String}@,System.Collections.Generic.Dictionary{System.String,System.Double}@)~System.Boolean")]
[assembly: SuppressMessage("Style", "IDE0066:Convert switch statement to expression", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:AnalysisManagerPhospho_FDR_AggregatorPlugIn.AnalysisToolRunnerPhosphoFdrAggregator.DetermineAScoreParamFilePath(System.String)~System.String")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable legacy name", Scope = "type", Target = "~T:AnalysisManagerPhospho_FDR_AggregatorPlugIn.AnalysisResourcesPhosphoFdrAggregator")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable legacy name", Scope = "type", Target = "~T:AnalysisManagerPhospho_FDR_AggregatorPlugIn.AnalysisToolRunnerPhosphoFdrAggregator")]
[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "member", Target = "~M:AnalysisManagerPhospho_FDR_AggregatorPlugIn.AnalysisToolRunnerPhosphoFdrAggregator.DetermineSpectrumFilePath(System.IO.DirectoryInfo)~System.String")]
