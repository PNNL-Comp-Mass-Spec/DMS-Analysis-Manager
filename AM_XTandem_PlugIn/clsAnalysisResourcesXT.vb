Imports AnalysisManagerBase
Imports PRISM.Logging
Imports System.IO
Imports System
Imports ParamFileGenerator.MakeParams
Imports AnalysisManagerMSMSResourceBase

Public Class clsAnalysisResourcesXT
	Inherits clsAnalysisResourcesMSMS

	Friend Const MOD_DEFS_FILE_SUFFIX As String = "_ModDefs.txt"
	Friend Const MASS_CORRECTION_TAGS_FILENAME As String = "Mass_Correction_Tags.txt"

	Protected Overrides Function RetrieveParamFile(ByVal ParamFileName As String, _
																	ByVal ParamFilePath As String, ByVal WorkDir As String) As Boolean

		Dim result As Boolean = True
		Dim ModDefsFileName As String

		' XTandem just copies its parameter file from the central repository
		'	This will eventually be replaced by Ken Auberry dll call to make param file on the fly

		'		result = result And CopyFileToWorkDir(ParamFileName, ParamFilePath, WorkDir)

		'Uses ParamFileGenerator dll provided by Ken Auberry to get mod_defs and mass correction files
		'NOTE: ParamFilePath isn't used in this override, but is needed in parameter list for compatability
		Dim ParFileGen As IGenerateFile = New clsMakeParameterFile

		result = ParFileGen.MakeFile(ParamFileName, SetBioworksVersion("xtandem"), _
			Path.Combine(m_mgrParams.GetParam("commonfileandfolderlocations", "orgdbdir"), m_jobParams.GetParam("organismDBName")), _
			WorkDir, m_mgrParams.GetParam("databasesettings", "connectionstring"))

		If Not result Then
			m_logger.PostEntry("Error converting param file: " & ParFileGen.LastError, ILogger.logMsgType.logError, True)
			Return False
		End If

		'get copies of taxonomy_base.xml, input_base.txt, and default_input.xml
		result = result And CopyFileToWorkDir("taxonomy_base.xml", ParamFilePath, WorkDir)
		result = result And CopyFileToWorkDir("input_base.txt", ParamFilePath, WorkDir)
		result = result And CopyFileToWorkDir("default_input.xml", ParamFilePath, WorkDir)

		' set up taxonomy file to reference the organism DB file (fasta)
		result = result And MakeTaxonomyFile()

		' set up run parameter file to reference spectra file, taxonomy file, and analysis parameter file
		result = result And MakeInputFile()

		RetrieveParamFile = result

	End Function

	Protected Function MakeTaxonomyFile() As Boolean
		Dim result As Boolean = True

		' set up taxonomy file to reference the organsim DB file (fasta)

		Dim WorkingDir As String = m_mgrParams.GetParam("commonfileandfolderlocations", "WorkDir")
		Dim OrgDBName As String = m_jobParams.GetParam("generatedFastaName")
		Dim OrganismName As String = m_jobParams.GetParam("OrganismName")
		Dim LocalOrgDBFolder As String = m_mgrParams.GetParam("commonfileandfolderlocations", "orgdbdir")
		Dim OrgFilePath As String = Path.Combine(LocalOrgDBFolder, OrgDBName)

		'edit base taxonomy file into actual
		Try
			' Create an instance of StreamWriter to write to a file.
			Dim inputFile As StreamWriter = New StreamWriter(Path.Combine(WorkingDir, "taxonomy.xml"))
			' Create an instance of StreamReader to read from a file.
			Dim inputBase As StreamReader = New StreamReader(Path.Combine(WorkingDir, "taxonomy_base.xml"))
			Dim inpLine As String
			Dim tmpFlag As Boolean
			' Read and display the lines from the file until the end 
			' of the file is reached.
			Do
				inpLine = inputBase.ReadLine()
				If Not inpLine Is Nothing Then
					inpLine = inpLine.Replace("ORGANISM_NAME", OrganismName)
					inpLine = inpLine.Replace("FASTA_FILE_PATH", OrgFilePath)
					inputFile.WriteLine(inpLine)
				End If
			Loop Until inpLine Is Nothing
			inputBase.Close()
			inputFile.Close()
		Catch E As Exception
			' Let the user know what went wrong.
			m_logger.PostError("The file could not be read", E, True)
		End Try

		'get rid of base file
		File.Delete(Path.Combine(WorkingDir, "taxonomy_base.xml"))

		Return result
	End Function

	Protected Function MakeInputFile() As Boolean
		Dim result As Boolean = True

		' set up input to reference spectra file, taxonomy file, and parameter file

		Dim WorkingDir As String = m_mgrParams.GetParam("commonfileandfolderlocations", "WorkDir")
		Dim OrganismName As String = m_jobParams.GetParam("OrganismName")
		Dim ParamFilePath As String = Path.Combine(WorkingDir, m_jobParams.GetParam("parmFileName"))
		Dim SpectrumFilePath As String = Path.Combine(WorkingDir, m_jobParams.GetParam("datasetNum") & "_dta.txt")
		Dim TaxonomyFilePath As String = Path.Combine(WorkingDir, "taxonomy.xml")
		Dim OutputFilePath As String = Path.Combine(WorkingDir, m_jobParams.GetParam("datasetNum") & "_xt.xml")

		'make input file
		'start by adding the contents of the parameter file.
		'replace substitution tags in input_base.txt with proper file path references
		'and add to input file (in proper XML format)
		Try
			' Create an instance of StreamWriter to write to a file.
			Dim inputFile As StreamWriter = New StreamWriter(Path.Combine(WorkingDir, "input.xml"))
			' Create an instance of StreamReader to read from a file.
			Dim inputBase As StreamReader = New StreamReader(Path.Combine(WorkingDir, "input_base.txt"))
			Dim paramFile As StreamReader = New StreamReader(ParamFilePath)
			Dim paramLine As String
			Dim inpLine As String
			Dim tmpFlag As Boolean
			' Read and display the lines from the file until the end 
			' of the file is reached.
			Do
				paramLine = paramFile.ReadLine()
				If paramLine Is Nothing Then
					Exit Do
				End If
				inputFile.WriteLine(paramLine)
				If paramLine.IndexOf("<bioml>") <> -1 Then
					Do
						inpLine = inputBase.ReadLine()
						If Not inpLine Is Nothing Then
							inpLine = inpLine.Replace("ORGANISM_NAME", OrganismName)
							inpLine = inpLine.Replace("TAXONOMY_FILE_PATH", TaxonomyFilePath)
							inpLine = inpLine.Replace("SPECTRUM_FILE_PATH", SpectrumFilePath)
							inpLine = inpLine.Replace("OUTPUT_FILE_PATH", OutputFilePath)
							inputFile.WriteLine(inpLine)
							tmpFlag = False
						End If
					Loop Until inpLine Is Nothing
				End If
			Loop Until paramLine Is Nothing
			inputBase.Close()
			inputFile.Close()
			paramFile.Close()
		Catch E As Exception
			' Let the user know what went wrong.
			m_logger.PostError("The file could not be read", E, True)
			result = False
		End Try

		'JDS end code

		'get rid of base file
		File.Delete(Path.Combine(WorkingDir, "input_base.txt"))

		Return result
	End Function

	Friend Shared Function ConstructModificationDefinitionsFilename(ByVal ParameterFileName As String) As String
		Return System.IO.Path.GetFileNameWithoutExtension(ParameterFileName) & MOD_DEFS_FILE_SUFFIX
	End Function

End Class
