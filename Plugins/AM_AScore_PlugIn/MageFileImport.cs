using System.Collections.Generic;
using Mage;

namespace AnalysisManager_AScore_PlugIn
{

	/// <summary>
	/// Simple Mage FileContentProcessor module
	/// that imports the contents of files that it receives via standard tabular input
	/// to the given SQLite database table
	/// </summary>
	public class MageFileImport : FileContentProcessor
	{

		#region Properties

		public string DBTableName { get; set; }
		public string DBFilePath { get; set; }
		public string ImportColumnList { get; set; }

		#endregion

		#region Constructors

		// constructor
		public MageFileImport()
		{
			base.SourceFolderColumnName = "Folder";
			base.SourceFileColumnName = "Name";
			base.OutputFolderPath = "ignore";
			base.OutputFileName = "ignore";
		}


		#endregion

		#region Overrides of Mage ContentFilter

		// import contents of given file to SQLite database table
		protected override void ProcessFile(string sourceFile, string sourcePath, string destPath, Dictionary<string, string> context)
		{
			if (string.IsNullOrEmpty(ImportColumnList))
			{
				clsAScoreMagePipeline.ImportFileToSQLite(sourcePath, DBFilePath, DBTableName);
			}
			else
			{
				clsAScoreMagePipeline.ImportFileToSQLiteWithColumnMods(sourcePath, DBFilePath, DBTableName, ImportColumnList, context);
			}
		}

		#endregion
	}

}
