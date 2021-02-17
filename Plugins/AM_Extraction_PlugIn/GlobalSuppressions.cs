// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not needed", Scope = "member", Target = "~M:AnalysisManagerExtractionPlugin.clsExtractToolRunner.PHRP_ProgressChanged(System.String,System.Single)")]
[assembly: SuppressMessage("Design", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "Ignore errors deleting this file", Scope = "member", Target = "~M:AnalysisManagerExtractionPlugin.clsPepHitResultsProcWrapper.ExtractDataFromResults(System.String,System.Boolean,System.Boolean,System.String,System.String)~AnalysisManagerBase.CloseOutType")]
[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "member", Target = "~M:AnalysisManagerExtractionPlugin.clsPepHitResultsProcWrapper.ExtractDataFromResults(System.String,System.Boolean,System.Boolean,System.String,System.String)~AnalysisManagerBase.CloseOutType")]
[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "member", Target = "~M:AnalysisManagerExtractionPlugin.clsPepHitResultsProcWrapper.ValidatePrimaryResultsFile(System.IO.FileInfo,System.String,System.String)~AnalysisManagerBase.CloseOutType")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable legacy name", Scope = "type", Target = "~T:AnalysisManagerExtractionPlugin.clsPepHitResultsProcWrapper")]
