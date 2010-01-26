Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesOM
    Inherits clsAnalysisResources

	Friend Const MOD_DEFS_FILE_SUFFIX As String = "_ModDefs.txt"
	Friend Const MASS_CORRECTION_TAGS_FILENAME As String = "Mass_Correction_Tags.txt"

    Public Overrides Function GetResources() As AnalysisManagerBase.IJobParams.CloseOutType

        Dim result As Boolean

        'Clear out list of files to delete or keep when packaging the results
        clsGlobal.ResetFilesToDeleteOrKeep()

        'Retrieve Fasta file
        If Not RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")) Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        ' OMSSA just copies its parameter file from the central repository
        '	This will eventually be replaced by Ken Auberry dll call to make param file on the fly

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting param file")

        'Retrieve param file
        If Not RetrieveGeneratedParamFile( _
         m_jobParams.GetParam("ParmFileName"), _
         m_jobParams.GetParam("ParmFileStoragePath"), _
         m_mgrParams.GetParam("workdir")) _
        Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        'Retrieve unzipped dta files (do not unconcatenate since OMSSA we will convert the _DTA.txt file to a _DTA.xml file, which OMSSA will read)
        If Not RetrieveDtaFiles(False) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Add all the extensions of the files to delete after run
        clsGlobal.m_FilesToDeleteExt.Add("_dta.zip") 'Zipped DTA
        clsGlobal.m_FilesToDeleteExt.Add("_dta.txt") 'Unzipped, concatenated DTA
        clsGlobal.m_FilesToDeleteExt.Add(".dta")  'DTA files

        Dim ext As String
        Dim DumFiles() As String

        'update list of files to be deleted after run
        For Each ext In clsGlobal.m_FilesToDeleteExt
            DumFiles = System.IO.Directory.GetFiles(m_mgrParams.GetParam("workdir"), "*" & ext) 'Zipped DTA
            For Each FileToDel As String In DumFiles
                clsGlobal.FilesToDelete.Add(FileToDel)
            Next
        Next

        Dim parmfilestore As String = m_jobParams.GetParam("ParmFileStoragePath")
        result = CopyFileToWorkDir("taxonomy_base.xml", m_jobParams.GetParam("ParmFileStoragePath"), m_mgrParams.GetParam("WorkDir"))
        If Not result Then
            Dim Msg As String = "clsAnalysisResourcesOM.GetResources(), failed retrieving taxonomy_base.xml file."
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If
        result = CopyFileToWorkDir("input_base.txt", m_jobParams.GetParam("ParmFileStoragePath"), m_mgrParams.GetParam("WorkDir"))
        If Not result Then
            Dim Msg As String = "clsAnalysisResourcesOM.GetResources(), failed retrieving input_base.xml file."
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        result = CopyFileToWorkDir("default_input.xml", m_jobParams.GetParam("ParmFileStoragePath"), m_mgrParams.GetParam("WorkDir"))
        If Not result Then
            Dim Msg As String = "clsAnalysisResourcesOM.GetResources(), failed retrieving default_input.xml file."
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' set up taxonomy file to reference the organism DB file (fasta)
        result = MakeTaxonomyFile()
        If Not result Then
            Dim Msg As String = "clsAnalysisResourcesOM.GetResources(), failed making taxonomy file."
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' set up run parameter file to reference spectra file, taxonomy file, and analysis parameter file
        result = MakeInputFile()
        If Not result Then
            Dim Msg As String = "clsAnalysisResourcesOM.GetResources(), failed making input file."
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Function MakeTaxonomyFile() As Boolean
        Dim result As Boolean = True

        ' set up taxonomy file to reference the organsim DB file (fasta)

        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")
        Dim OrgDBName As String = m_jobParams.GetParam("generatedFastaName")
        Dim OrganismName As String = m_jobParams.GetParam("OrganismName")
        Dim LocalOrgDBFolder As String = m_mgrParams.GetParam("orgdbdir")
        Dim OrgFilePath As String = System.IO.Path.Combine(LocalOrgDBFolder, OrgDBName)

        'edit base taxonomy file into actual
        Try
            ' Create an instance of StreamWriter to write to a file.
            Dim inputFile As System.IO.StreamWriter = New System.IO.StreamWriter(System.IO.Path.Combine(WorkingDir, "taxonomy.xml"))
            ' Create an instance of StreamReader to read from a file.
            Dim inputBase As System.IO.StreamReader = New System.IO.StreamReader(System.IO.Path.Combine(WorkingDir, "taxonomy_base.xml"))
            Dim inpLine As String
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
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesOM.MakeTaxonomyFile, The file could not be read" & E.Message)
        End Try

        'get rid of base file
        System.IO.File.Delete(System.IO.Path.Combine(WorkingDir, "taxonomy_base.xml"))

        Return result
    End Function

    Protected Function MakeInputFile() As Boolean
        Dim result As Boolean = True

        ' set up input to reference spectra file, taxonomy file, and parameter file

        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")
        Dim OrganismName As String = m_jobParams.GetParam("OrganismName")
        Dim ParamFilePath As String = System.IO.Path.Combine(WorkingDir, m_jobParams.GetParam("parmFileName"))
        Dim SpectrumFilePath As String = System.IO.Path.Combine(WorkingDir, m_jobParams.GetParam("datasetNum") & "_dta.txt")
        Dim TaxonomyFilePath As String = System.IO.Path.Combine(WorkingDir, "taxonomy.xml")
        Dim OutputFilePath As String = System.IO.Path.Combine(WorkingDir, m_jobParams.GetParam("datasetNum") & "_om.omx")

        'make input file
        'start by adding the contents of the parameter file.
        'replace substitution tags in input_base.txt with proper file path references
        'and add to input file (in proper XML format)
        Try
            ' Create an instance of StreamWriter to write to a file.
            Dim inputFile As System.IO.StreamWriter = New System.IO.StreamWriter(System.IO.Path.Combine(WorkingDir, "input.xml"))
            ' Create an instance of StreamReader to read from a file.
            Dim inputBase As System.IO.StreamReader = New System.IO.StreamReader(System.IO.Path.Combine(WorkingDir, "input_base.txt"))
            Dim paramFile As System.IO.StreamReader = New System.IO.StreamReader(ParamFilePath)
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
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clxAnalysisResourcesOM.MakeInputFile, The file could not be read" & E.Message)
            result = False
        End Try

        'get rid of base file
        System.IO.File.Delete(System.IO.Path.Combine(WorkingDir, "input_base.txt"))

        Return result
    End Function

    Friend Shared Function ConstructModificationDefinitionsFilename(ByVal ParameterFileName As String) As String
        Return System.IO.Path.GetFileNameWithoutExtension(ParameterFileName) & MOD_DEFS_FILE_SUFFIX
    End Function

End Class
