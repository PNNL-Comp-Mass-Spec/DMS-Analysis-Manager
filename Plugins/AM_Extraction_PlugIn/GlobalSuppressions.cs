// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("CodeQuality", "IDE0079:Remove unnecessary suppression", Justification = "Ignore", Scope = "member", Target = "~M:AnalysisManagerExtractionPlugin.AnalysisResourcesExtraction.RetrieveMiscFiles(System.String)~AnalysisManagerBase.JobConfig.CloseOutType")]
[assembly: SuppressMessage("CodeQuality", "IDE0051:Remove unused private members", Justification = "Leave for reference", Scope = "member", Target = "~M:AnalysisManagerExtractionPlugin.PeptideExtractWrapper.ExtractTools_CurrentProgress(System.Double)")]
[assembly: SuppressMessage("Design", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "Ignore errors here", Scope = "member", Target = "~M:AnalysisManagerExtractionPlugin.ExtractToolRunner.RunPhrpForInSpecT~AnalysisManagerBase.JobConfig.CloseOutType")]
[assembly: SuppressMessage("Design", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "Ignore errors here", Scope = "member", Target = "~M:AnalysisManagerExtractionPlugin.ExtractToolRunner.RunPhrpForMSGFPlus~AnalysisManagerBase.JobConfig.CloseOutType")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not needed", Scope = "member", Target = "~M:AnalysisManagerExtractionPlugin.ExtractToolRunner.PHRP_ProgressChanged(System.String,System.Single)")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not needed", Scope = "member", Target = "~M:AnalysisManagerExtractionPlugin.PHRPMassErrorValidator.ValidatePHRPResultMassErrors(System.String,PHRPReader.PeptideHitResultTypes,System.String)~System.Boolean")]
[assembly: SuppressMessage("Roslynator", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not needed", Scope = "member", Target = "~M:AnalysisManagerExtractionPlugin.PHRPMassErrorValidator.ExaminePHRPResults(System.String,PHRPReader.PeptideHitResultTypes,System.String,System.Collections.Generic.SortedDictionary{System.Double,System.String}@,System.Double@,System.Int32@,System.Int32@,System.Boolean@)~System.Boolean")]
[assembly: SuppressMessage("Simplification", "RCS1179:Unnecessary assignment.", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:AnalysisManagerExtractionPlugin.AnalysisResourcesExtraction.CheckAScoreRequired(System.String,System.String)~System.Boolean")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable legacy name", Scope = "type", Target = "~T:AnalysisManagerExtractionPlugin.PepHitResultsProcWrapper")]
[assembly: SuppressMessage("Usage", "RCS1146:Use conditional access.", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:AnalysisManagerExtractionPlugin.ExtractToolRunner.RunMzidMerger(System.String,System.String)~AnalysisManagerBase.JobConfig.CloseOutType")]
[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "member", Target = "~M:AnalysisManagerExtractionPlugin.PepHitResultsProcWrapper.ValidatePrimaryResultsFile(System.IO.FileInfo,System.String,System.String)~AnalysisManagerBase.JobConfig.CloseOutType")]
