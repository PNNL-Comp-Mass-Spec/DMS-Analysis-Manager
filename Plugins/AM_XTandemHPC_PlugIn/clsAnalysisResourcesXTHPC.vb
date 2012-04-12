Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesXTHPC
    Inherits clsAnalysisResources

    Friend Const MOD_DEFS_FILE_SUFFIX As String = "_ModDefs.txt"
    Friend Const INPUT_FILE_PREFIX As String = "Input_Part"
    Friend Const MASS_CORRECTION_TAGS_FILENAME As String = "Mass_Correction_Tags.txt"
    Friend Const TAXONOMY_FILENAME As String = "taxonomy.xml"
    Friend Const DEFAULT_INPUT As String = "default_input.xml"

    ' The HPC account name is currently hard-coded; we may need to retrieve this from DMS on a per-job basis

    ' User proposal emsl33210 expired April 1, 2010
    'Friend Const HPC_ACCOUNT_NAME As String = "emsl33210"

    ' This is the MPP3 idle account (enabled June 2, 2010 by Erich Vorpagel)
    ' As of October 2010 it is linked to EMSL proposal 34708 with 80,000 hours
    Friend Const HPC_ACCOUNT_NAME As String = "mscfidle"

    Private WithEvents mCDTACondenser As CondenseCDTAFile.clsCDTAFileCondenser

    Public Overrides Function GetResources() As IJobParams.CloseOutType

        Dim result As Boolean

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
        m_JobParams.AddResultFileExtensionToSkip("_dta.zip") 'Zipped DTA
        m_JobParams.AddResultFileExtensionToSkip("_dta.txt") 'Unzipped, concatenated DTA
        m_JobParams.AddResultFileExtensionToSkip(".dta")  'DTA files

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
        Dim OrgDBName As String = m_jobParams.GetParam("PeptideSearch", "generatedFastaName")
        Dim OrganismName As String = m_jobParams.GetParam("OrganismName")
        Dim LocalOrgDBFolder As String = m_mgrParams.GetParam("orgdbdir")
        Dim OrgFilePath As String = System.IO.Path.Combine(clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY & "fasta/", OrgDBName)

        'edit base taxonomy file into actual
        Try
            ' Create an instance of StreamWriter to write to a file.
            Dim swOut As System.IO.StreamWriter = New System.IO.StreamWriter(System.IO.Path.Combine(WorkingDir, TAXONOMY_FILENAME))
            ' Create an instance of StreamReader to read from a file.
            Dim inputBase As System.IO.StreamReader = New System.IO.StreamReader(System.IO.Path.Combine(WorkingDir, "taxonomy_base.xml"))
            Dim strOut As String
            ' Read and display the lines from the file until the end 
            ' of the file is reached.
            Do
                strOut = inputBase.ReadLine()
                If Not strOut Is Nothing Then
                    strOut = strOut.Replace("ORGANISM_NAME", OrganismName)
                    strOut = strOut.Replace("FASTA_FILE_PATH", OrgFilePath)
                    WriteUnix(swOut, strOut)
                End If
            Loop Until strOut Is Nothing
            inputBase.Close()
            swOut.Close()
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
        Dim intNumClonedSteps As Integer
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
            intNumClonedSteps = CInt(m_jobParams.GetParam("NumberOfClonedSteps"))

            For i = 1 To intNumClonedSteps
                Input_Filename = System.IO.Path.Combine(WorkingDir, INPUT_FILE_PREFIX & i & ".xml")
                Msub_Filename = System.IO.Path.Combine(WorkingDir, "X-Tandem_Job" & JobNum & "_" & i & ".msub")
                Start_Filename = System.IO.Path.Combine(WorkingDir, "StartXT_Job" & JobNum & "_" & i)
                Put_CmdFile = System.IO.Path.Combine(WorkingDir, "PutCmds_Job" & JobNum & "_" & i)
                m_JobParams.AddResultFileExtensionToSkip(System.IO.Path.GetFileName(Put_CmdFile))
                MakeInputFile(Input_Filename, CStr(i))
                MakeMSubFile(Msub_Filename, CStr(i))
                MakeStartFile(Start_Filename, Msub_Filename, CStr(i))
                MakePutFilesCmdFile(Put_CmdFile, Msub_Filename, CStr(i))
            Next

            Get_FastaFileList_CmdFile = System.IO.Path.Combine(WorkingDir, "CreateFastaFileList.txt")
            MakeListFastaFilesCmdFile(Get_FastaFileList_CmdFile, JobNum)
            m_JobParams.AddResultFileExtensionToSkip(System.IO.Path.GetFileName(Get_FastaFileList_CmdFile))
            m_JobParams.AddResultFileExtensionToSkip("fastafiles.txt")

            Create_FastaFileList_CmdFile = System.IO.Path.Combine(WorkingDir, "GetFastaFileList.txt")
            MakeGetFastaFilesListCmdFile(Create_FastaFileList_CmdFile, JobNum)
            m_JobParams.AddResultFileExtensionToSkip(System.IO.Path.GetFileName(Create_FastaFileList_CmdFile))

            CreateDir_CmdFile = System.IO.Path.Combine(WorkingDir, "CreateDir_Job" & JobNum)
            MakeCreateDirectorysCmdFile(CreateDir_CmdFile)
            m_JobParams.AddResultFileExtensionToSkip(System.IO.Path.GetFileName(CreateDir_CmdFile))

            RemoveDir_CmdFile = System.IO.Path.Combine(WorkingDir, "Remove_Job" & JobNum)
            MakeRemoveDirectorysCmdFile(RemoveDir_CmdFile)
            m_JobParams.AddResultFileExtensionToSkip(System.IO.Path.GetFileName(RemoveDir_CmdFile))

            Put_CmdFastaFile = System.IO.Path.Combine(WorkingDir, "PutFasta_Job" & JobNum)
            MakePutFastaCmdFile(Put_CmdFastaFile)
            m_JobParams.AddResultFileExtensionToSkip(System.IO.Path.GetFileName(Put_CmdFastaFile))

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
            Dim swOut As System.IO.StreamWriter = New System.IO.StreamWriter(inputFilename)
            ' Create an instance of StreamReader to read from a file.
            Dim inputBase As System.IO.StreamReader = New System.IO.StreamReader(System.IO.Path.Combine(WorkingDir, "input_base.txt"))
            Dim paramFile As System.IO.StreamReader = New System.IO.StreamReader(ParamFilePath)
            Dim paramLine As String
            Dim strOut As String

            ' Read and display the lines from the file until the end 
            ' of the file is reached.
            Do
                paramLine = paramFile.ReadLine()
                If paramLine Is Nothing Then
                    Exit Do
                End If
                WriteUnix(swOut, paramLine)
                If paramLine.IndexOf("<bioml>") <> -1 Then
                    Do
                        strOut = inputBase.ReadLine()
                        If Not strOut Is Nothing Then
                            strOut = strOut.Replace("ORGANISM_NAME", OrganismName)
                            strOut = strOut.Replace("TAXONOMY_FILE_PATH", TAXONOMY_FILENAME)
                            strOut = strOut.Replace("SPECTRUM_FILE_PATH", SpectrumFilePath)
                            strOut = strOut.Replace("OUTPUT_FILE_PATH", OutputFilePath)
                            WriteUnix(swOut, strOut)
                        End If
                    Loop Until strOut Is Nothing
                End If
            Loop Until paramLine Is Nothing
            inputBase.Close()
            swOut.Close()
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

        Dim strNumCloneSteps As String
        Dim intNumClonedSteps As Integer

        Dim i As Integer
        Dim DtaResultFolderName As String

        Dim WorkingDir As String
        Dim DatasetName As String

        Try
            WorkingDir = m_mgrParams.GetParam("WorkDir")
            DatasetName = m_jobParams.GetParam("DatasetNum")


            strNumCloneSteps = m_jobParams.GetParam("ParallelInspect", "NumberOfClonedSteps")

            If strNumCloneSteps Is Nothing OrElse _
               strNumCloneSteps.Length = 0 OrElse _
               Not Integer.TryParse(strNumCloneSteps, intNumClonedSteps) Then

                ' Error determining the number of cloned steps
                ' Set the value to 1 and update m_jobParams
                intNumClonedSteps = 1
                m_jobParams.AddAdditionalParameter("ParallelInspect", "NumberOfClonedSteps", intNumClonedSteps.ToString)
            End If

            'Determine the number of parallelized steps
            If intNumClonedSteps > 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Processing HPC XTandem with " & intNumClonedSteps.ToString & " segments")
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Processing HPC XTandem")
            End If

            ' Normally the DTA_Split tool will have been run before this step tool
            ' Even if NumberOfClonedSteps is 1, the output file will be named Dataset_1_dta.txt
            ' Thus, we'll first look for that file
            DtaResultFileName = DatasetName & "_1_dta.txt"
            DtaResultFolderName = FindDataFile(DtaResultFileName)

            If DtaResultFolderName = "" Then
                ' No folder found containing the _1_dta.txt file (error will have already been logged)
                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "FindDataFile returned False for " & DtaResultFileName)
                End If

                ' If the DTA_Split tool was not run first, then we should look for a _dta.zip file
                DtaResultFileName = DatasetName & "_dta.zip"
                DtaResultFolderName = FindDataFile(DtaResultFileName)

                If DtaResultFolderName = "" Then
                    ' No folder found containing the _dta.zip file (error will have already been logged)
                    If m_DebugLevel >= 3 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "FindDataFile returned False for " & DtaResultFileName)
                    End If

                    Return False
                Else

                    Return RetrieveZippedDtaFile(WorkingDir, DatasetName, DtaResultFolderName, DtaResultFileName)
                End If
            End If

            ' Get each of the _dta.txt files
            ' Example names: 
            '   Dataset_1_dta.txt
            '   Dataset_2_dta.txt
            '   Dataset_3_dta.txt

            For i = 1 To intNumClonedSteps

                DtaResultFileName = DatasetName & "_" & i.ToString & "_dta.txt"

                'Copy the file
                If Not CopyFileToWorkDir(DtaResultFileName, DtaResultFolderName, WorkingDir) Then
                    ' Error copying file (error will have already been logged)
                    If m_DebugLevel >= 3 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CopyFileToWorkDir returned False for " & DtaResultFileName & " using folder " & DtaResultFolderName)
                    End If
                    Return False
                End If

                ' If the _dta.txt file is over 2 GB in size, then condense it
                If Not ValidateDTATextFileSize(WorkingDir, DtaResultFileName) Then
                    'Errors were reported in function call, so just return
                    Return False
                End If
            Next


        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in clsAnalysisResourcesXT.RetrieveDtaFiles: " & ex.Message)
            Return False
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Copies file DtaResultFileName from SourceFolderPath to WorkingDir
    ''' </summary>
    ''' <param name="WorkingDir"></param>
    ''' <param name="DatasetName"></param>
    ''' <param name="SourceFolderPath"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function RetrieveZippedDtaFile(ByVal WorkingDir As String, _
                                             ByVal DatasetName As String, _
                                             ByVal SourceFolderPath As String, _
                                             ByVal ZippedDTAFileName As String) As Boolean

        Dim fiDTAFile As System.IO.FileInfo
        Dim strNewPath As String

        Dim result As Boolean

        Try
            'Copy the file
            If Not CopyFileToWorkDir(ZippedDTAFileName, SourceFolderPath, WorkingDir) Then
                ' Error copying file (error will have already been logged)
                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CopyFileToWorkDir returned False for " & ZippedDTAFileName & " using folder " & SourceFolderPath)
                End If
                Return False
            End If

            ' Unzip the file
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Unzipping concatenated DTA file")
            If UnzipFileStart(System.IO.Path.Combine(WorkingDir, ZippedDTAFileName), WorkingDir, "clsAnalysisResourcesXTHPC.RetrieveZippedDtaFile", False) Then
                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Concatenated DTA file unzipped")
                End If
            End If

            ' Rename the file to end in _1_dta.txt
            fiDTAFile = New System.IO.FileInfo(System.IO.Path.Combine(WorkingDir, DatasetName & "_dta.txt"))

            strNewPath = System.IO.Path.Combine(WorkingDir, DatasetName & "_1_dta.txt")
            fiDTAFile.MoveTo(strNewPath)

            ' If the _dta.txt file is over 2 GB in size, then condense it
            If Not ValidateDTATextFileSize(WorkingDir, System.IO.Path.GetFileName(strNewPath)) Then
                'Errors were reported in function call, so just return
                Return False
            End If

            result = True

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in clsAnalysisResourcesXT.RetrieveZippedDtaFile: " & ex.Message)
            result = False
        End Try

        Return result

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
            Dim swOut As System.IO.StreamWriter = New System.IO.StreamWriter(inputFilename)
            Dim strOut As String

            WriteUnix(swOut, "#Note: Use ""gbalance -u svc-dms"" to find valid accounts")

            WriteUnix(swOut)

            WriteUnix(swOut, "#msub syntax:")

            WriteUnix(swOut, "#msub msubFile -A emslProposalNum")

            WriteUnix(swOut, "#")

            WriteUnix(swOut, "#The following command uses redirection to save both the output and")

            WriteUnix(swOut, "# any error messages to file msub.output")

            WriteUnix(swOut)

            strOut = "cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY & "Job" & JobNum & "_" & File_Index & "/"
            WriteUnix(swOut, strOut)

            m_jobParams.AddAdditionalParameter("ParallelInspect", "HPCAccountName", HPC_ACCOUNT_NAME)

            strOut = "/apps/moab/current/bin/msub ../Job" & JobNum & "_msub" & File_Index & "/" & MsubFilename & " -A " & HPC_ACCOUNT_NAME & " > ../Job" & JobNum & "_msub" & File_Index & "/" & MsubOutFilename & " 2>&1"
            WriteUnix(swOut, strOut)

            swOut.Close()
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

        Dim intNumClonedSteps As Integer

        Dim i As Integer

        Try

            ' Create an instance of StreamWriter to write to a file.
            Dim swOut As System.IO.StreamWriter = New System.IO.StreamWriter(inputFilename)

            intNumClonedSteps = CInt(m_jobParams.GetParam("NumberOfClonedSteps"))

            WriteUnix(swOut, "cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY)

            For i = 1 To intNumClonedSteps
                WriteUnix(swOut, "mkdir Job" & JobNum & "_" & i)
                WriteUnix(swOut, "mkdir Job" & JobNum & "_msub" & i)
            Next

            swOut.Close()

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
            Dim swOut As System.IO.StreamWriter = New System.IO.StreamWriter(inputFilename)

            WriteUnix(swOut, "cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY & "Job" & JobNum & "_" & File_Index & "/")

            WriteUnix(swOut, "put " & WorkingDir & "\Input_Part" & File_Index & ".xml")

            WriteUnix(swOut, "put " & WorkingDir & "\" & m_jobParams.GetParam("DatasetNum") & "_" & File_Index & "_dta.txt")

            WriteUnix(swOut, "put " & WorkingDir & "\" & TAXONOMY_FILENAME)

            WriteUnix(swOut, "put " & WorkingDir & "\" & DEFAULT_INPUT)

            WriteUnix(swOut, "put " & WorkingDir & "\" & MASS_CORRECTION_TAGS_FILENAME)

            WriteUnix(swOut, "put " & WorkingDir & "\" & m_jobParams.GetParam("ParmFileName"))

            WriteUnix(swOut, "put " & WorkingDir & "\" & System.IO.Path.GetFileNameWithoutExtension(m_jobParams.GetParam("ParmFileName")) & MOD_DEFS_FILE_SUFFIX)

            WriteUnix(swOut, "cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY & "Job" & JobNum & "_msub" & File_Index & "/")

            WriteUnix(swOut, "put " & WorkingDir & "\StartXT_Job" & JobNum & "_" & File_Index)

            WriteUnix(swOut, "put " & WorkingDir & "\X-Tandem_Job" & JobNum & "_" & File_Index & ".msub")

            swOut.Close()

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

        Dim OrgDBName As String = m_jobParams.GetParam("PeptideSearch", "generatedFastaName")

        Dim JobNum As String = m_jobParams.GetParam("Job")

        Try
            ' Create an instance of StreamWriter to write to a file.
            Dim swOut As System.IO.StreamWriter = New System.IO.StreamWriter(inputFilename)

            WriteUnix(swOut, "cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY & "fasta/")

            WriteUnix(swOut, "put " & LocalOrgDBFolder & "\" & OrgDBName)

            swOut.Close()

        Catch E As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXT.MakePutFastaCmdFile, The file could not be written" & E.Message)
            result = False
            Return result
        End Try

        Return result
    End Function

    Protected Function MakeMSubFile(ByVal inputFilename As String, ByVal File_Index As String) As Boolean
        Const PPN_VALUE As Integer = 8

        Dim result As Boolean = True
        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")
        Dim JobNum As String = m_jobParams.GetParam("Job")
        Dim HPCNodeCount As String = m_jobParams.GetParam("HPCNodeCount")
        Dim HPCMaxHours As String = m_jobParams.GetParam("HPCMaxHours")

        Dim intNodeCount As Integer
        Dim intTotalCores As Integer

        Dim dblWallTimeHours As Double
        Dim intDays As Integer
        Dim intHours As Integer
        Dim intMinutes As Integer
        Dim WallTimeText As String

        Try
            intNodeCount = CInt(HPCNodeCount)
            intTotalCores = intNodeCount * PPN_VALUE

            If intNodeCount = 1 Then
                ' Always use a wall-time value of 30 minutes when only using one node
                WallTimeText = "00:30:00"
            Else
                ' Convert the number of hours specified by HPCMaxHours into a string; examples:
                '  HPCMaxHours of 2   will become   02:00:00
                '  HPCMaxHours of 2.5 will become   02:30:00
                '  HPCMaxHours of 18  will become   18:00:00
                '  HPCMaxHours of 24  will become 1:00:00:00
                '  HPCMaxHours of 30  will become 1:06:00:00

                dblWallTimeHours = CDbl(HPCMaxHours)

                If dblWallTimeHours >= 24 Then
                    intDays = CInt(Math.Floor(dblWallTimeHours / 24))
                    dblWallTimeHours -= intDays * 24
                Else
                    intDays = 0
                End If

                intHours = CInt(Math.Floor(dblWallTimeHours))

                dblWallTimeHours -= intHours
                If dblWallTimeHours < 0 Then dblWallTimeHours = 0
                intMinutes = CInt(dblWallTimeHours * 60)
                If intMinutes >= 60 Then
                    intHours += 1
                    intMinutes = 0
                End If

                If intDays > 0 Then
                    WallTimeText = intDays.ToString("0") & ":"
                Else
                    WallTimeText = String.Empty
                End If
                WallTimeText &= intHours.ToString("00") & ":" & intMinutes.ToString("00") & ":00"

            End If

            ' Create an instance of StreamWriter to write to a file.
            Dim swOut As System.IO.StreamWriter = New System.IO.StreamWriter(inputFilename)
            Dim strOut As String

            WriteUnix(swOut, "#!/bin/bash")

            ' Number of nodes, number of processors (cores) per node (ppn), and max job length ("hh:mm:ss")
            strOut = "#MSUB -l nodes=" & HPCNodeCount & ":ppn=" & PPN_VALUE & ",walltime=" & WallTimeText
            WriteUnix(swOut, strOut)

            strOut = "#MSUB -o " & JobNum & "_Part" & File_Index & ".output.%j"
            WriteUnix(swOut, strOut)

            strOut = "#MSUB -e " & JobNum & "_Part" & File_Index & ".err.%j"
            WriteUnix(swOut, strOut)

            WriteUnix(swOut, "#MSUB -V")

            WriteUnix(swOut)

            WriteUnix(swOut, "source /etc/profile.d/modules.sh")

            WriteUnix(swOut, "source /home/scicons/bin/set_modulepath.sh")

            WriteUnix(swOut, "export MODULEPATH=""$MODULEPATH:/home/dmlb2000/modulefiles""")

            WriteUnix(swOut, "module purge")

            WriteUnix(swOut, "module load python")

            WriteUnix(swOut, "module load gcc/4.2.4")

            WriteUnix(swOut, "module load mvapich2/1.4")

            WriteUnix(swOut)

            WriteUnix(swOut, "export LD_LIBRARY_PATH=""/home/svc-dms/x-tandem/install/lib:$LD_LIBRARY_PATH""")

            strOut = "srun -n " & intTotalCores & " -N " & HPCNodeCount & " /home/svc-dms/x-tandem/parallel_tandem_08-12-01/bin/tandem.exe Input_Part" & File_Index & ".xml"
            WriteUnix(swOut, strOut)

            swOut.Close()

            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "MSub file created; will reserve " & intNodeCount.ToString & " nodes (" & intTotalCores.ToString & " cores) for a maximum WallTime of " & WallTimeText)
            End If

        Catch ex As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXT.MakeMSubFile, Error generating msub file: " & ex.Message)
            result = False
            Return result
        End Try

        Return result
    End Function

    Protected Sub WriteUnix(ByRef swOut As System.IO.StreamWriter)
        WriteUnix(swOut, String.Empty)
    End Sub

    Protected Sub WriteUnix(ByRef swOut As System.IO.StreamWriter, ByVal inputString As String)

        swOut.Write(inputString & ControlChars.Lf)

    End Sub

    Protected Function MakeListFastaFilesCmdFile(ByVal inputFilename As String, ByVal JobNum As String) As Boolean
        Dim result As Boolean = True

        Try
            ' Create an instance of StreamWriter to write to a file.
            Dim swOut As System.IO.StreamWriter = New System.IO.StreamWriter(inputFilename)

            WriteUnix(swOut, "cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY & "fasta/")

            WriteUnix(swOut, "ls -lrt " & m_jobParams.GetParam("PeptideSearch", "generatedFastaName") & " | awk '{print $5}' > fastafiles_Job" & JobNum & ".txt")

            swOut.Close()

        Catch E As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXT.MakeListFastaFilesCmdFile, The file could not be written" & E.Message)
            result = False
            Return result
        End Try

        Return result
    End Function

    Protected Function MakeGetFastaFilesListCmdFile(ByVal inputFilename As String, ByVal JobNum As String) As Boolean
        Dim result As Boolean = True

        Try
            ' Create an instance of StreamWriter to write to a file.
            Dim swOut As System.IO.StreamWriter = New System.IO.StreamWriter(inputFilename)

            WriteUnix(swOut, "cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY & "fasta/")

            WriteUnix(swOut, "get fastafiles_Job" & JobNum & ".txt")

            swOut.Close()

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

        Dim intNumClonedSteps As Integer

        Dim i As Integer

        Try

            ' Create an instance of StreamWriter to write to a file.
            Dim swOut As System.IO.StreamWriter = New System.IO.StreamWriter(inputFilename)

            intNumClonedSteps = CInt(m_jobParams.GetParam("NumberOfClonedSteps"))

            WriteUnix(swOut, "cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY)

            For i = 1 To intNumClonedSteps
                WriteUnix(swOut, "rm Job" & JobNum & "_" & i & "/* Job" & JobNum & "_" & i & "/.*")
                WriteUnix(swOut, "rmdir Job" & JobNum & "_" & i)
                WriteUnix(swOut, "rm Job" & JobNum & "_msub" & i & "/* Job" & JobNum & "_" & i & "/.*")
                WriteUnix(swOut, "rmdir Job" & JobNum & "_msub" & i)
            Next

            WriteUnix(swOut, "rm fasta/fastafiles_Job" & JobNum & ".txt")

            swOut.Close()

        Catch E As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXT.MakeCreateDirectorysCmdFile, The file could not be written" & E.Message)
            result = False
            Return result
        End Try

        Return result
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
