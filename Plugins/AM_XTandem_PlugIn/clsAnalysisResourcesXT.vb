Option Strict On

' Last modified 06/15/2009 JDS - Added logging using log4net
Imports AnalysisManagerBase

Public Class clsAnalysisResourcesXT
    Inherits clsAnalysisResources

	Friend Const MOD_DEFS_FILE_SUFFIX As String = "_ModDefs.txt"
	Friend Const MASS_CORRECTION_TAGS_FILENAME As String = "Mass_Correction_Tags.txt"

    Private WithEvents mCDTACondenser As CondenseCDTAFile.clsCDTAFileCondenser

    Public Overrides Function GetResources() As IJobParams.CloseOutType

        Dim result As Boolean
        Dim strWorkDir As String

        'Retrieve Fasta file
        If Not RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")) Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        ' XTandem just copies its parameter file from the central repository
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting param file")

        strWorkDir = m_mgrParams.GetParam("workdir")

        'Retrieve param file
        If Not RetrieveGeneratedParamFile( _
         m_jobParams.GetParam("ParmFileName"), _
         m_jobParams.GetParam("ParmFileStoragePath"), _
         strWorkDir) _
        Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        'Retrieve unzipped dta files (do not unconcatenate since X!Tandem uses the _Dta.txt file directly)
        If Not RetrieveDtaFiles(False) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Add all the extensions of the files to delete after run
        m_JobParams.AddResultFileExtensionToSkip("_dta.zip") 'Zipped DTA
        m_JobParams.AddResultFileExtensionToSkip("_dta.txt") 'Unzipped, concatenated DTA
        m_JobParams.AddResultFileExtensionToSkip(".dta")  'DTA files

        ' If the _dta.txt file is over 2 GB in size, then condense it

        If Not ValidateDTATextFileSize(strWorkDir, m_jobParams.GetParam("datasetNum") & "_dta.txt") Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Dim parmfilestore As String = m_jobParams.GetParam("ParmFileStoragePath")
        result = CopyFileToWorkDir("taxonomy_base.xml", m_jobParams.GetParam("ParmFileStoragePath"), strWorkDir)
        If Not result Then
            Dim Msg As String = "clsAnalysisResourcesXT.GetResources(), failed retrieving taxonomy_base.xml file."
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If
        result = CopyFileToWorkDir("input_base.txt", m_jobParams.GetParam("ParmFileStoragePath"), strWorkDir)
        If Not result Then
            Dim Msg As String = "clsAnalysisResourcesXT.GetResources(), failed retrieving input_base.xml file."
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        result = CopyFileToWorkDir("default_input.xml", m_jobParams.GetParam("ParmFileStoragePath"), strWorkDir)
        If Not result Then
            Dim Msg As String = "clsAnalysisResourcesXT.GetResources(), failed retrieving default_input.xml file."
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' set up taxonomy file to reference the organism DB file (fasta)
        result = MakeTaxonomyFile()
        If Not result Then
            Dim Msg As String = "clsAnalysisResourcesXT.GetResources(), failed making taxonomy file."
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' set up run parameter file to reference spectra file, taxonomy file, and analysis parameter file
        result = MakeInputFile()
        If Not result Then
            Dim Msg As String = "clsAnalysisResourcesXT.GetResources(), failed making input file."
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Function MakeTaxonomyFile() As Boolean
        Dim result As Boolean = True

        ' set up taxonomy file to reference the organsim DB file (fasta)

        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")
        Dim OrgDBName As String = m_jobParams.GetParam("PeptideSearch", "generatedFastaName")
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
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXT.MakeTaxonomyFile, The file could not be read" & E.Message)
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
        Dim OutputFilePath As String = System.IO.Path.Combine(WorkingDir, m_jobParams.GetParam("datasetNum") & "_xt.xml")

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
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clxAnalysisResourcesXT.MakeInputFile, The file could not be read" & E.Message)
            result = False
        End Try

        'get rid of base file
        System.IO.File.Delete(System.IO.Path.Combine(WorkingDir, "input_base.txt"))

        Return result
    End Function

    Friend Shared Function ConstructModificationDefinitionsFilename(ByVal ParameterFileName As String) As String
        Return System.IO.Path.GetFileNameWithoutExtension(ParameterFileName) & MOD_DEFS_FILE_SUFFIX
    End Function

    Protected Function ValidateDTATextFileSize(ByVal strWorkDir As String, ByVal strInputFileName As String) As Boolean
        Const FILE_SIZE_THRESHOLD As Integer = Int32.MaxValue

        Dim ioFileInfo As System.IO.FileInfo
        Dim strInputFilePath As String
        Dim strFilePathOld As String

        Dim strMessage As String

        Dim blnSuccess As Boolean

        Try
            strInputFilePath = System.IO.Path.Combine(strWorkDir, strInputFileName)
            ioFileInfo = New System.IO.FileInfo(strInputFilePath)

            If Not ioFileInfo.Exists Then
                m_message = "_DTA.txt file not found: " & strInputFilePath
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return False
            End If

            If ioFileInfo.Length >= FILE_SIZE_THRESHOLD Then
                ' Need to condense the file

                strMessage = ioFileInfo.Name & " is " & CSng(ioFileInfo.Length / 1024 / 1024 / 1024).ToString("0.00") & " GB in size; will now condense it by combining data points with consecutive zero-intensity values"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strMessage)

                mCDTACondenser = New CondenseCDTAFile.clsCDTAFileCondenser

                blnSuccess = mCDTACondenser.ProcessFile(ioFileInfo.FullName, ioFileInfo.DirectoryName)

                If Not blnSuccess Then
                    m_message = "Error condensing _DTA.txt file: " & mCDTACondenser.GetErrorMessage()
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                    Return False
                Else
                    ' Wait 500 msec, then check the size of the new _dta.txt file
                    System.Threading.Thread.Sleep(500)

                    ioFileInfo.Refresh()

                    If m_DebugLevel >= 1 Then
                        strMessage = "Condensing complete; size of the new _dta.txt file is " & CSng(ioFileInfo.Length / 1024 / 1024 / 1024).ToString("0.00") & " GB"
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strMessage)
                    End If

                    Try
                        strFilePathOld = System.IO.Path.Combine(strWorkDir, System.IO.Path.GetFileNameWithoutExtension(ioFileInfo.FullName) & "_Old.txt")

                        If m_DebugLevel >= 2 Then
                            strMessage = "Now deleting file " & strFilePathOld
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strMessage)
                        End If

                        ioFileInfo = New System.IO.FileInfo(strFilePathOld)
                        If ioFileInfo.Exists Then
                            ioFileInfo.Delete()
                        Else
                            strMessage = "Old _DTA.txt file not found:" & ioFileInfo.FullName & "; cannot delete"
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strMessage)
                        End If

                    Catch ex As Exception
                        ' Error deleting the file; log it but keep processing
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception deleting _dta_old.txt file: " & ex.Message)
                    End Try

                End If
            End If

            blnSuccess = True

        Catch ex As Exception
            m_message = "Exception in ValidateDTATextFileSize"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

        Return blnSuccess

    End Function

    Private Sub mCDTACondenser_ProgressChanged(ByVal taskDescription As String, ByVal percentComplete As Single) Handles mCDTACondenser.ProgressChanged
        Static dtLastUpdateTime As System.DateTime

        If m_DebugLevel >= 1 Then
            If m_DebugLevel = 1 AndAlso System.DateTime.UtcNow.Subtract(dtLastUpdateTime).TotalSeconds >= 60 OrElse _
               m_DebugLevel > 1 AndAlso System.DateTime.UtcNow.Subtract(dtLastUpdateTime).TotalSeconds >= 20 Then
                dtLastUpdateTime = System.DateTime.UtcNow

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... " & percentComplete.ToString("0.00") & "% complete")
            End If
        End If
    End Sub

End Class
