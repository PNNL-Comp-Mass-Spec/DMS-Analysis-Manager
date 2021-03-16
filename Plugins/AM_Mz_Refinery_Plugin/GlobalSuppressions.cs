// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Design", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "Silently ignore a file deletion error in this case", Scope = "member", Target = "~M:AnalysisManagerMzRefineryPlugIn.AnalysisToolRunnerMzRefinery.CopyFailedResultsToArchiveFolder(System.String)")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerMzRefineryPlugIn.AnalysisResourcesMzRefinery")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerMzRefineryPlugIn.AnalysisToolRunnerMzRefinery")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerMzRefineryPlugIn.AnalysisToolRunnerMzRefinery.MzRefinerProgRunnerMode")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerMzRefineryPlugIn.MzRefineryMassErrorStatsExtractor")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerMzRefineryPlugIn.MassErrorInfo")]
[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "module")]
