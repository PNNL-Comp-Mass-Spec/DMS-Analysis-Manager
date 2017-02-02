Option Strict On

Imports AnalysisManagerBase
Imports System.IO

Public Class clsAnalysisResourcesXT
    Inherits clsAnalysisResources

    Friend Const MOD_DEFS_FILE_SUFFIX As String = "_ModDefs.txt"
    Friend Const MASS_CORRECTION_TAGS_FILENAME As String = "Mass_Correction_Tags.txt"

    Private WithEvents mCDTACondenser As CondenseCDTAFile.clsCDTAFileCondenser

    Public Overrides Sub Setup(mgrParams As IMgrParams, jobParams As IJobParams, statusTools As IStatusFile, myEMSLUtilities As clsMyEMSLUtilities)
        MyBase.Setup(mgrParams, jobParams, statusTools, myEmslUtilities)
        SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, True)
    End Sub

    Public Overrides Function GetResources() As IJobParams.CloseOutType

        ' Retrieve shared resources, including the JobParameters file from the previous job step
        Dim result = GetSharedResources()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        'Retrieve Fasta file
        If Not RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")) Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        ' XTandem just copies its parameter file from the central repository
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting param file")

        'Retrieve param file
        If Not RetrieveGeneratedParamFile(m_jobParams.GetParam("ParmFileName")) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Retrieve the _DTA.txt file
        ' Note that if the file was found in MyEMSL then RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file
        If Not RetrieveDtaFiles() Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Make sure the _DTA.txt file has parent ion lines with text: scan=x and cs=y
        ' X!Tandem uses this information to determine the scan number
        Dim strCDTAPath As String = Path.Combine(m_WorkingDir, m_DatasetName & "_dta.txt")
        Const blnReplaceSourceFile = True
        Const blnDeleteSourceFileIfUpdated = True

        If Not ValidateCDTAFileScanAndCSTags(strCDTAPath, blnReplaceSourceFile, blnDeleteSourceFileIfUpdated, "") Then
            m_message = "Error validating the _DTA.txt file"
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Add all the extensions of the files to delete after run
        m_jobParams.AddResultFileExtensionToSkip("_dta.zip") 'Zipped DTA
        m_jobParams.AddResultFileExtensionToSkip("_dta.txt") 'Unzipped, concatenated DTA
        m_jobParams.AddResultFileExtensionToSkip(".dta")  'DTA files

        ' If the _dta.txt file is over 2 GB in size, then condense it

        If Not ValidateDTATextFileSize(m_WorkingDir, m_DatasetName & "_dta.txt") Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Dim success = CopyFileToWorkDir("taxonomy_base.xml", m_jobParams.GetParam("ParmFileStoragePath"), m_WorkingDir)
        If Not success Then
            Const Msg = "clsAnalysisResourcesXT.GetResources(), failed retrieving taxonomy_base.xml file."
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        success = CopyFileToWorkDir("input_base.txt", m_jobParams.GetParam("ParmFileStoragePath"), m_WorkingDir)
        If Not success Then
            Const Msg = "clsAnalysisResourcesXT.GetResources(), failed retrieving input_base.xml file."
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        success = CopyFileToWorkDir("default_input.xml", m_jobParams.GetParam("ParmFileStoragePath"), m_WorkingDir)
        If Not success Then
            Const Msg = "clsAnalysisResourcesXT.GetResources(), failed retrieving default_input.xml file."
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' set up taxonomy file to reference the organism DB file (fasta)
        success = MakeTaxonomyFile()
        If Not success Then
            Const Msg = "clsAnalysisResourcesXT.GetResources(), failed making taxonomy file."
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' set up run parameter file to reference spectra file, taxonomy file, and analysis parameter file
        success = MakeInputFile()
        If Not success Then
            Const Msg = "clsAnalysisResourcesXT.GetResources(), failed making input file."
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Function MakeTaxonomyFile() As Boolean

        ' set up taxonomy file to reference the organsim DB file (fasta)

        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")
        Dim OrgDBName As String = m_jobParams.GetParam("PeptideSearch", "generatedFastaName")
        Dim OrganismName As String = m_jobParams.GetParam("OrganismName")
        Dim LocalOrgDBFolder As String = m_mgrParams.GetParam("orgdbdir")
        Dim OrgFilePath As String = Path.Combine(LocalOrgDBFolder, OrgDBName)

        'edit base taxonomy file into actual
        Try
            ' Create an instance of StreamWriter to write to a file.
            Dim inputFile = New StreamWriter(Path.Combine(WorkingDir, "taxonomy.xml"))

            ' Create an instance of StreamReader to read from a file.
            Dim inputBase = New StreamReader(Path.Combine(WorkingDir, "taxonomy_base.xml"))
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
        File.Delete(Path.Combine(WorkingDir, "taxonomy_base.xml"))

        Return True
    End Function

    Protected Function MakeInputFile() As Boolean
        Dim result = True

        ' set up input to reference spectra file, taxonomy file, and parameter file

        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")
        Dim OrganismName As String = m_jobParams.GetParam("OrganismName")
        Dim ParamFilePath As String = Path.Combine(WorkingDir, m_jobParams.GetParam("parmFileName"))
        Dim SpectrumFilePath As String = Path.Combine(WorkingDir, m_DatasetName & "_dta.txt")
        Dim TaxonomyFilePath As String = Path.Combine(WorkingDir, "taxonomy.xml")
        Dim OutputFilePath As String = Path.Combine(WorkingDir, m_DatasetName & "_xt.xml")

        'make input file
        'start by adding the contents of the parameter file.
        'replace substitution tags in input_base.txt with proper file path references
        'and add to input file (in proper XML format)
        Try
            ' Create an instance of StreamWriter to write to a file.
            Dim inputFile = New StreamWriter(Path.Combine(WorkingDir, "input.xml"))
            ' Create an instance of StreamReader to read from a file.
            Dim inputBase = New StreamReader(Path.Combine(WorkingDir, "input_base.txt"))
            Dim paramFile = New StreamReader(ParamFilePath)
            Dim paramLine As String
            Dim inpLine As String

            ' Read and display the lines from the file until the end 
            ' of the file is reached.
            Do
                paramLine = paramFile.ReadLine()
                If paramLine Is Nothing Then
                    Exit Do
                End If
                inputFile.WriteLine(paramLine)
                If paramLine.IndexOf("<bioml>", StringComparison.Ordinal) <> -1 Then
                    Do
                        inpLine = inputBase.ReadLine()
                        If Not inpLine Is Nothing Then
                            inpLine = inpLine.Replace("ORGANISM_NAME", OrganismName)
                            inpLine = inpLine.Replace("TAXONOMY_FILE_PATH", TaxonomyFilePath)
                            inpLine = inpLine.Replace("SPECTRUM_FILE_PATH", SpectrumFilePath)
                            inpLine = inpLine.Replace("OUTPUT_FILE_PATH", OutputFilePath)
                            inputFile.WriteLine(inpLine)
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
        File.Delete(Path.Combine(WorkingDir, "input_base.txt"))

        Return result
    End Function

    Friend Shared Function ConstructModificationDefinitionsFilename(ParameterFileName As String) As String
        Return Path.GetFileNameWithoutExtension(ParameterFileName) & MOD_DEFS_FILE_SUFFIX
    End Function

    Protected Function ValidateDTATextFileSize(strWorkDir As String, strInputFileName As String) As Boolean
        Const FILE_SIZE_THRESHOLD As Integer = Int32.MaxValue

        Dim ioFileInfo As FileInfo
        Dim strInputFilePath As String
        Dim strFilePathOld As String

        Dim strMessage As String

        Dim blnSuccess As Boolean

        Try
            strInputFilePath = Path.Combine(strWorkDir, strInputFileName)
            ioFileInfo = New FileInfo(strInputFilePath)

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
                    Threading.Thread.Sleep(500)

                    ioFileInfo.Refresh()

                    If m_DebugLevel >= 1 Then
                        strMessage = "Condensing complete; size of the new _dta.txt file is " & CSng(ioFileInfo.Length / 1024 / 1024 / 1024).ToString("0.00") & " GB"
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strMessage)
                    End If

                    Try
                        strFilePathOld = Path.Combine(strWorkDir, Path.GetFileNameWithoutExtension(ioFileInfo.FullName) & "_Old.txt")

                        If m_DebugLevel >= 2 Then
                            strMessage = "Now deleting file " & strFilePathOld
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strMessage)
                        End If

                        ioFileInfo = New FileInfo(strFilePathOld)
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

    Private Sub mCDTACondenser_ProgressChanged(taskDescription As String, percentComplete As Single) Handles mCDTACondenser.ProgressChanged
        Static dtLastUpdateTime As DateTime

        If m_DebugLevel >= 1 Then
            If m_DebugLevel = 1 AndAlso DateTime.UtcNow.Subtract(dtLastUpdateTime).TotalSeconds >= 60 OrElse
               m_DebugLevel > 1 AndAlso DateTime.UtcNow.Subtract(dtLastUpdateTime).TotalSeconds >= 20 Then
                dtLastUpdateTime = DateTime.UtcNow

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... " & percentComplete.ToString("0.00") & "% complete")
            End If
        End If
    End Sub

End Class
