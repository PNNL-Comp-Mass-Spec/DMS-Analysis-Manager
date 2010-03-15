Option Strict On

Imports AnalysisManagerBase
Imports System.Globalization

Public Class clsAnalysisResourcesXTHPC
    Inherits clsAnalysisResources

    Friend Const MOD_DEFS_FILE_SUFFIX As String = "_ModDefs.txt"
    Friend Const INPUT_FILE_PREFIX As String = "Input_Part"
    Friend Const MASS_CORRECTION_TAGS_FILENAME As String = "Mass_Correction_Tags.txt"
    Friend Const TAXONOMY_FILENAME As String = "taxonomy.xml"
    Friend Const DEFAULT_INPUT As String = "default_input.xml"

    Public Overrides Function GetResources() As AnalysisManagerBase.IJobParams.CloseOutType

        Dim result As Boolean

        'Clear out list of files to delete or keep when packaging the results
        clsGlobal.ResetFilesToDeleteOrKeep()

        'Retrieve Fasta file
        If Not RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")) Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        ' XTandem just copies its parameter file from the central repository
        '	This will eventually be replaced by Ken Auberry dll call to make param file on the fly

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting param file")

        'Retrieve param file
        If Not RetrieveGeneratedParamFile( _
         m_jobParams.GetParam("ParmFileName"), _
         m_jobParams.GetParam("ParmFileStoragePath"), _
         m_mgrParams.GetParam("workdir")) _
        Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        'Retrieve unzipped dta files (do not unconcatenate since X!Tandem uses the _Dta.txt file directly)
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
            Dim Msg As String = "clsAnalysisResourcesXT.GetResources(), failed retrieving taxonomy_base.xml file."
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If
        result = CopyFileToWorkDir("input_base.txt", m_jobParams.GetParam("ParmFileStoragePath"), m_mgrParams.GetParam("WorkDir"))
        If Not result Then
            Dim Msg As String = "clsAnalysisResourcesXT.GetResources(), failed retrieving input_base.xml file."
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        result = CopyFileToWorkDir(DEFAULT_INPUT, m_jobParams.GetParam("ParmFileStoragePath"), m_mgrParams.GetParam("WorkDir"))
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
        result = MakeInputFiles()
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
        Dim OrgDBName As String = m_jobParams.GetParam("generatedFastaName")
        Dim OrganismName As String = m_jobParams.GetParam("OrganismName")
        Dim LocalOrgDBFolder As String = m_mgrParams.GetParam("orgdbdir")
        Dim OrgFilePath As String = System.IO.Path.Combine(clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY & "fasta/", OrgDBName)

        'edit base taxonomy file into actual
        Try
            ' Create an instance of StreamWriter to write to a file.
            Dim inputFile As System.IO.StreamWriter = New System.IO.StreamWriter(System.IO.Path.Combine(WorkingDir, TAXONOMY_FILENAME))
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
                    inputFile.Write(WriteUnix(inpLine))
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

    Protected Function MakeInputFiles() As Boolean
        Dim result As Boolean = True
        Dim ParallelZipNum As Integer
        Dim Input_Filename As String
        Dim Msub_Filename As String
        Dim Start_Filename As String
        Dim Put_CmdFile As String
        Dim Put_CmdFastaFile As String
        Dim CreateDir_CmdFile As String
        Dim RemoveDir_CmdFile As String
        Dim Get_FastaFileList_CmdFile As String
        Dim Create_FastaFileList_CmdFile As String
        Dim i As Integer
        Dim JobNum As String = m_jobParams.GetParam("Job")

        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")
        Try
            ParallelZipNum = CInt(m_jobParams.GetParam("NumberOfClonedSteps"))

            For i = 1 To ParallelZipNum
                Input_Filename = System.IO.Path.Combine(WorkingDir, INPUT_FILE_PREFIX & i & ".xml")
                Msub_Filename = System.IO.Path.Combine(WorkingDir, "X-Tandem_Job" & JobNum & "_" & i & ".msub")
                Start_Filename = System.IO.Path.Combine(WorkingDir, "StartXT_Job" & JobNum & "_" & i)
                Put_CmdFile = System.IO.Path.Combine(WorkingDir, "PutCmds_Job" & JobNum & "_" & i)
                clsGlobal.m_FilesToDeleteExt.Add(System.IO.Path.GetFileName(Put_CmdFile))
                MakeInputFile(Input_Filename, CStr(i))
                MakeMSubFile(Msub_Filename, CStr(i))
                MakeStartFile(Start_Filename, Msub_Filename, CStr(i))
                MakePutFilesCmdFile(Put_CmdFile, Msub_Filename, CStr(i))
            Next

            Get_FastaFileList_CmdFile = System.IO.Path.Combine(WorkingDir, "CreateFastaFileList.txt")
            MakeListFastaFilesCmdFile(Get_FastaFileList_CmdFile)
            clsGlobal.m_FilesToDeleteExt.Add(System.IO.Path.GetFileName(Get_FastaFileList_CmdFile))

            Create_FastaFileList_CmdFile = System.IO.Path.Combine(WorkingDir, "GetFastaFileList.txt")
            MakeGetFastaFilesListCmdFile(Create_FastaFileList_CmdFile)
            clsGlobal.m_FilesToDeleteExt.Add(System.IO.Path.GetFileName(Create_FastaFileList_CmdFile))

            CreateDir_CmdFile = System.IO.Path.Combine(WorkingDir, "CreateDir_Job" & JobNum)
            MakeCreateDirectorysCmdFile(CreateDir_CmdFile)
            clsGlobal.m_FilesToDeleteExt.Add(System.IO.Path.GetFileName(CreateDir_CmdFile))

            RemoveDir_CmdFile = System.IO.Path.Combine(WorkingDir, "Remove_Job" & JobNum)
            MakeRemoveDirectorysCmdFile(RemoveDir_CmdFile)
            clsGlobal.m_FilesToDeleteExt.Add(System.IO.Path.GetFileName(RemoveDir_CmdFile))

            Put_CmdFastaFile = System.IO.Path.Combine(WorkingDir, "PutFastaJob" & JobNum)
            MakePutFastaCmdFile(Put_CmdFastaFile)
            clsGlobal.m_FilesToDeleteExt.Add(System.IO.Path.GetFileName(Put_CmdFastaFile))

            'get rid of base file
            System.IO.File.Delete(System.IO.Path.Combine(WorkingDir, "input_base.txt"))

        Catch E As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clxAnalysisResourcesXT.MakeInputFiles, Error occurred while creating input file(s)" & E.Message)
            result = False
        End Try

        Return result
    End Function

    Protected Function MakeInputFile(ByVal inputFilename As String, ByVal File_Index As String) As Boolean
        Dim result As Boolean = True

        ' set up input to reference spectra file, taxonomy file, and parameter file

        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")
        Dim OrganismName As String = m_jobParams.GetParam("OrganismName")
        Dim ParamFilePath As String = System.IO.Path.Combine(WorkingDir, m_jobParams.GetParam("parmFileName"))
        Dim SpectrumFilePath As String = m_jobParams.GetParam("datasetNum") & "_" & File_Index & "_dta.txt"
        Dim OutputFilePath As String = m_jobParams.GetParam("datasetNum") & "_" & File_Index & "_xt.xml"

        'make input file
        'start by adding the contents of the parameter file.
        'replace substitution tags in input_base.txt with proper file path references
        'and add to input file (in proper XML format)
        Try
            ' Create an instance of StreamWriter to write to a file.
            Dim inputFile As System.IO.StreamWriter = New System.IO.StreamWriter(inputFilename)
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
                            inpLine = inpLine.Replace("TAXONOMY_FILE_PATH", TAXONOMY_FILENAME)
                            inpLine = inpLine.Replace("SPECTRUM_FILE_PATH", SpectrumFilePath)
                            inpLine = inpLine.Replace("OUTPUT_FILE_PATH", OutputFilePath)
                            inputFile.Write(WriteUnix(inpLine))
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

        Return result
    End Function

    Friend Shared Function ConstructModificationDefinitionsFilename(ByVal ParameterFileName As String) As String
        Return System.IO.Path.GetFileNameWithoutExtension(ParameterFileName) & MOD_DEFS_FILE_SUFFIX
    End Function

    ''' <summary>
    ''' Retrieves zipped, concatenated DTA file, unzips, and splits into individual DTA files
    ''' </summary>
    ''' <param name="UnConcatenate">TRUE to split concatenated file; FALSE to leave the file concatenated</param>
    ''' <returns>TRUE for success, FALSE for error</returns>
    ''' <remarks></remarks>
    Public Overrides Function RetrieveDtaFiles(ByVal UnConcatenate As Boolean) As Boolean

        'Retrieve zipped DTA file
        Dim DtaResultFileName As String
        Dim strUnzippedFileNameRoot As String
        Dim strPathToDelete As String = String.Empty

        Dim NumCloneSteps As String
        Dim stepNum As String
        Dim parallelZipNum As Integer
        Dim isParallelized As Boolean = False
        Dim i As Integer
        Dim DtaResultFolderName As String

        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")

        NumCloneSteps = m_jobParams.GetParam("NumberOfClonedSteps")
        stepNum = m_jobParams.GetParam("Step")

        'Determine the number of parallelized steps
        If CInt(NumCloneSteps) = 1 Then
            DtaResultFileName = m_jobParams.GetParam("DatasetNum") & "_dta.txt"

            DtaResultFolderName = FindDataFile(DtaResultFileName)

            If DtaResultFolderName = "" Then
                ' No folder found containing the zipped DTA files (error will have already been logged)
                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "FindDataFile returned False for " & DtaResultFileName)
                End If
                Return False
            End If

            strUnzippedFileNameRoot = m_jobParams.GetParam("DatasetNum")
            'Copy the file
            If Not CopyFileToWorkDir(DtaResultFileName, DtaResultFolderName, WorkingDir) Then
                ' Error copying file (error will have already been logged)
                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CopyFileToWorkDir returned False for " & DtaResultFileName & " using folder " & DtaResultFolderName)
                End If
                Return False
            End If
        Else
            parallelZipNum = CInt(NumCloneSteps)
            isParallelized = True
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Processing parallelized Inspect segment " & parallelZipNum.ToString)

            DtaResultFileName = m_jobParams.GetParam("DatasetNum") & "_1_dta.txt"

            DtaResultFolderName = FindDataFile(DtaResultFileName)

            If DtaResultFolderName = "" Then
                ' No folder found containing the zipped DTA files (error will have already been logged)
                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "FindDataFile returned False for " & DtaResultFileName)
                End If
                Return False
            End If

            For i = 1 To parallelZipNum
                'parallelZipNum += parallelZipNum
                DtaResultFileName = m_jobParams.GetParam("DatasetNum") & "_" & i & "_dta.txt"
                strUnzippedFileNameRoot = m_jobParams.GetParam("DatasetNum") & "_" & i
                'Copy the file
                If Not CopyFileToWorkDir(DtaResultFileName, DtaResultFolderName, WorkingDir) Then
                    ' Error copying file (error will have already been logged)
                    If m_DebugLevel >= 3 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CopyFileToWorkDir returned False for " & DtaResultFileName & " using folder " & DtaResultFolderName)
                    End If
                    Return False
                End If
            Next
        End If


        ' Don't know if this is still valid for running on the Super computer - Leaving it in.
        'Check to see if the job is parallelized
        '  If it is parallelized, we do not need to unzip the concatenated DTA file (since it is already unzipped)
        '  If not parallelized, then we do need to unzip
        'If Not isParallelized OrElse System.IO.Path.GetExtension(DtaResultFileName).ToLower = ".zip" Then
        '    'Unzip concatenated DTA file
        '    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Unzipping concatenated DTA file")
        '    If UnzipFileStart(System.IO.Path.Combine(WorkingDir, DtaResultFileName), WorkingDir, "clsAnalysisResources.RetrieveDtaFiles", False) Then
        '        If m_DebugLevel >= 1 Then
        '            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Concatenated DTA file unzipped")
        '        End If
        '    End If
        'End If

        'Unconcatenate DTA file if needed
        'If UnConcatenate Then
        '    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Splitting concatenated DTA file")
        '    Dim BackWorker As New System.ComponentModel.BackgroundWorker
        '    Dim FileSplitter As New clsSplitCattedFiles(BackWorker)
        '    '				FileSplitter.SplitCattedDTAsOnly(m_jobParams.GetParam("DatasetNum"), WorkingDir)
        '    FileSplitter.SplitCattedDTAsOnly(strUnzippedFileNameRoot, WorkingDir)

        '    If m_DebugLevel >= 1 Then
        '        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Completed splitting concatenated DTA file")
        '    End If

        '    Try
        '        ' Now that the _dta.txt has been deconcatenated, we need to delete it; if we don't, Inspect will search it too

        '        strPathToDelete = System.IO.Path.Combine(WorkingDir, strUnzippedFileNameRoot & "_dta.txt")

        '        If m_DebugLevel >= 2 Then
        '            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting concatenated DTA file: " & strPathToDelete)
        '        End If

        '        System.Threading.Thread.Sleep(1000)
        '        System.IO.File.Delete(strPathToDelete)

        '        If System.IO.File.Exists(strPathToDelete) Then
        '            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Deletion of concatenated DTA file failed: " & strPathToDelete)
        '        End If

        '    Catch ex As Exception
        '        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception trying to delete file " & strPathToDelete & "; " & ex.Message)
        '    End Try
        'End If

        Return True

    End Function

    Protected Function MakeStartFile(ByVal inputFilename As String, ByVal MsubFilename As String, ByVal File_Index As String) As Boolean
        Dim result As Boolean = True

        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")

        Dim JobNum As String = m_jobParams.GetParam("Job")

        Dim MsubOutFilename As String

        Try

            MsubOutFilename = System.IO.Path.GetFileNameWithoutExtension(MsubFilename) & ".output"

            MsubFilename = System.IO.Path.GetFileName(MsubFilename)

            ' Create an instance of StreamWriter to write to a file.
            Dim inputFile As System.IO.StreamWriter = New System.IO.StreamWriter(inputFilename)
            Dim inpLine As String

            inputFile.Write(WriteUnix("#Note: Use ""gbalance -u svc-dms"" to find valid accounts"))

            inputFile.Write(ControlChars.Lf)

            inputFile.Write(WriteUnix("#msub syntax:"))

            inputFile.Write(WriteUnix("#msub msubFile -A emslProposalNum"))

            inputFile.Write(WriteUnix("#"))

            inputFile.Write(WriteUnix("#The following command uses redirection to save both the output and"))

            inputFile.Write(WriteUnix("# any error messages to file msub.output"))

            inputFile.Write(ControlChars.Lf)

            inpLine = "cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY & "Job" & JobNum & "_" & File_Index & "/"
            inputFile.Write(WriteUnix(inpLine))

            inpLine = "msub ../Job" & JobNum & "_msub" & File_Index & "/" & MsubFilename & " -A emsl33210 > ../Job" & JobNum & "_msub" & File_Index & "/" & MsubOutFilename & " 2>&1"
            inputFile.Write(WriteUnix(inpLine))

            inputFile.Close()
        Catch E As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXT.MakeStartFile, The file could not be read" & E.Message)
            result = False
            Return result
        End Try

        Return result
    End Function

    Protected Function MakeCreateDirectorysCmdFile(ByVal inputFilename As String) As Boolean
        Dim result As Boolean = True

        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")

        Dim JobNum As String = m_jobParams.GetParam("Job")

        Dim ParallelZipNum As Integer

        Dim i As Integer

        Try

            ' Create an instance of StreamWriter to write to a file.
            Dim inputFile As System.IO.StreamWriter = New System.IO.StreamWriter(inputFilename)

            ParallelZipNum = CInt(m_jobParams.GetParam("NumberOfClonedSteps"))

            inputFile.Write(WriteUnix("cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY))

            For i = 1 To ParallelZipNum
                inputFile.Write(WriteUnix("mkdir Job" & JobNum & "_" & i))
                inputFile.Write(WriteUnix("mkdir Job" & JobNum & "_msub" & i))
            Next

            inputFile.Close()

        Catch E As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXT.MakeCreateDirectorysCmdFile, The file could not be written" & E.Message)
            result = False
            Return result
        End Try

        Return result
    End Function

    Protected Function MakePutFilesCmdFile(ByVal inputFilename As String, ByVal MsubFilename As String, ByVal File_Index As String) As Boolean
        Dim result As Boolean = True

        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")

        Dim JobNum As String = m_jobParams.GetParam("Job")

        Try
            ' Create an instance of StreamWriter to write to a file.
            Dim inputFile As System.IO.StreamWriter = New System.IO.StreamWriter(inputFilename)

            inputFile.Write(WriteUnix("cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY & "Job" & JobNum & "_" & File_Index & "/"))

            inputFile.Write(WriteUnix("put " & WorkingDir & "\Input_Part" & File_Index & ".xml"))

            inputFile.Write(WriteUnix("put " & WorkingDir & "\" & m_jobParams.GetParam("DatasetNum") & "_" & File_Index & "_dta.txt"))

            inputFile.Write(WriteUnix("put " & WorkingDir & "\" & TAXONOMY_FILENAME))

            inputFile.Write(WriteUnix("put " & WorkingDir & "\" & DEFAULT_INPUT))

            inputFile.Write(WriteUnix("put " & WorkingDir & "\" & MASS_CORRECTION_TAGS_FILENAME))

            inputFile.Write(WriteUnix("put " & WorkingDir & "\" & m_jobParams.GetParam("ParmFileName")))

            inputFile.Write(WriteUnix("put " & WorkingDir & "\" & System.IO.Path.GetFileNameWithoutExtension(m_jobParams.GetParam("ParmFileName")) & MOD_DEFS_FILE_SUFFIX))

            inputFile.Write(WriteUnix("cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY & "Job" & JobNum & "_msub" & File_Index & "/"))

            inputFile.Write(WriteUnix("put " & WorkingDir & "\StartXT_Job" & JobNum & "_" & File_Index))

            inputFile.Write(WriteUnix("put " & WorkingDir & "\X-Tandem_Job" & JobNum & "_" & File_Index & ".msub"))

            inputFile.Close()

        Catch E As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXT.MakePutFilesCmdFile, The file could not be written" & E.Message)
            result = False
            Return result
        End Try

        Return result
    End Function

    Protected Function MakePutFastaCmdFile(ByVal inputFilename As String) As Boolean
        Dim result As Boolean = True

        Dim LocalOrgDBFolder As String = m_mgrParams.GetParam("orgdbdir")

        Dim OrgDBName As String = m_jobParams.GetParam("generatedFastaName")

        Dim JobNum As String = m_jobParams.GetParam("Job")

        Try
            ' Create an instance of StreamWriter to write to a file.
            Dim inputFile As System.IO.StreamWriter = New System.IO.StreamWriter(inputFilename)

            inputFile.Write(WriteUnix("cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY & "fasta/"))

            inputFile.Write(WriteUnix("put " & LocalOrgDBFolder & "\" & OrgDBName))

            inputFile.Close()

        Catch E As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXT.MakePutFastaCmdFile, The file could not be written" & E.Message)
            result = False
            Return result
        End Try

        Return result
    End Function

    Protected Function MakeMSubFile(ByVal inputFilename As String, ByVal File_Index As String) As Boolean
        Dim result As Boolean = True
        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")
        Dim JobNum As String = m_jobParams.GetParam("Job")
        Dim HPCNodeCount As String = m_jobParams.GetParam("HPCNodeCount")
        Dim HPCMaxHours As String = m_jobParams.GetParam("HPCMaxHours")
        Dim HPCNodeCountValue As Integer
        Dim WallTime As Date = CDate("1/1/2010")
        Dim WallTimeMax As Date = CDate("1/1/2010").AddHours(CDbl(HPCMaxHours))
        Dim WallTimeResult As String
        Dim i As Integer

        Try
            'Calculate the wall time
            For i = 1 To CInt(HPCNodeCount)
                WallTime = WallTime.AddMinutes(30)
                If WallTime = WallTimeMax Then
                    Exit For
                End If
            Next
            WallTimeResult = WallTime.ToString("T", CultureInfo.CreateSpecificCulture("fr-FR"))

            HPCNodeCountValue = CInt(HPCNodeCount) * 8

            ' Create an instance of StreamWriter to write to a file.
            Dim inputFile As System.IO.StreamWriter = New System.IO.StreamWriter(inputFilename)
            Dim inpLine As String

            inputFile.Write(WriteUnix("#!/bin/bash"))

            inpLine = "#MSUB -l nodes=" & HPCNodeCount & ":ppn=8,walltime=" & WallTimeResult
            inputFile.Write(WriteUnix(inpLine))

            inpLine = "#MSUB -o " & JobNum & "_Part" & File_Index & ".output.%j"
            inputFile.Write(WriteUnix(inpLine))

            inpLine = "#MSUB -e " & JobNum & "_Part" & File_Index & ".err.%j"
            inputFile.Write(WriteUnix(inpLine))

            inputFile.Write(WriteUnix("#MSUB -V"))

            inputFile.Write(ControlChars.Lf)

            inputFile.Write(WriteUnix("source /etc/profile.d/modules.sh"))

            inputFile.Write(WriteUnix("source /home/scicons/bin/set_modulepath.sh"))

            inputFile.Write(WriteUnix("export MODULEPATH=""$MODULEPATH:/home/dmlb2000/modulefiles"""))

            inputFile.Write(WriteUnix("module purge"))

            inputFile.Write(WriteUnix("module load python"))

            inputFile.Write(WriteUnix("module load gcc/4.2.4"))

            inputFile.Write(WriteUnix("module load mvapich2/1.4"))

            inputFile.Write(ControlChars.Lf)

            inputFile.Write(WriteUnix("export LD_LIBRARY_PATH=""/home/svc-dms/x-tandem/install/lib:$LD_LIBRARY_PATH"""))

            inpLine = "srun -n " & HPCNodeCountValue & " -N " & HPCNodeCount & " /home/svc-dms/x-tandem/parallel_tandem_08-12-01/bin/tandem.exe Input_Part" & File_Index & ".xml"
            inputFile.Write(WriteUnix(inpLine))

            inputFile.Close()
        Catch E As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXT.MakeMSubFile, The file could not be read" & E.Message)
            result = False
            Return result
        End Try

        Return result
    End Function

    Protected Function WriteUnix(ByVal inputString As String) As String

        inputString = inputString & ControlChars.Lf

        Return inputString

    End Function

    Protected Function MakeListFastaFilesCmdFile(ByVal inputFilename As String) As Boolean
        Dim result As Boolean = True

        Try
            ' Create an instance of StreamWriter to write to a file.
            Dim inputFile As System.IO.StreamWriter = New System.IO.StreamWriter(inputFilename)

            inputFile.Write(WriteUnix("cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY & "fasta/"))

            inputFile.Write(WriteUnix("ls -lrt " & m_jobParams.GetParam("generatedFastaName") & " | awk '{print $5}' > fastafiles.txt"))

            inputFile.Close()

        Catch E As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXT.MakeListFastaFilesCmdFile, The file could not be written" & E.Message)
            result = False
            Return result
        End Try

        Return result
    End Function

    Protected Function MakeGetFastaFilesListCmdFile(ByVal inputFilename As String) As Boolean
        Dim result As Boolean = True

        Try
            ' Create an instance of StreamWriter to write to a file.
            Dim inputFile As System.IO.StreamWriter = New System.IO.StreamWriter(inputFilename)

            inputFile.Write(WriteUnix("cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY & "fasta/"))

            inputFile.Write(WriteUnix("get fastafiles.txt"))

            inputFile.Close()

        Catch E As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXT.MakeGetFastaFilesListCmdFile, The file could not be written" & E.Message)
            result = False
            Return result
        End Try

        Return result
    End Function

    Protected Function MakeRemoveDirectorysCmdFile(ByVal inputFilename As String) As Boolean
        Dim result As Boolean = True

        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")

        Dim JobNum As String = m_jobParams.GetParam("Job")

        Dim ParallelZipNum As Integer

        Dim i As Integer

        Try

            ' Create an instance of StreamWriter to write to a file.
            Dim inputFile As System.IO.StreamWriter = New System.IO.StreamWriter(inputFilename)

            ParallelZipNum = CInt(m_jobParams.GetParam("NumberOfClonedSteps"))

            inputFile.Write(WriteUnix("cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY))

            For i = 1 To ParallelZipNum
                inputFile.Write(WriteUnix("rm Job" & JobNum & "_" & i & "/* Job" & JobNum & "_" & i & "/.*"))
                inputFile.Write(WriteUnix("rmdir Job" & JobNum & "_" & i))
                inputFile.Write(WriteUnix("rm Job" & JobNum & "_msub" & i & "/* Job" & JobNum & "_" & i & "/.*"))
                inputFile.Write(WriteUnix("rmdir Job" & JobNum & "_msub" & i))
            Next

            inputFile.Close()

        Catch E As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXT.MakeCreateDirectorysCmdFile, The file could not be written" & E.Message)
            result = False
            Return result
        End Try

        Return result
    End Function

End Class
