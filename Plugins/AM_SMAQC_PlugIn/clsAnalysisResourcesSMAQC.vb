Option Strict On

Imports AnalysisManagerBase
Imports System.IO
Imports PHRPReader

Public Class clsAnalysisResourcesSMAQC
    Inherits clsAnalysisResources

    Public Overrides Function GetResources() As CloseOutType

        ' Retrieve shared resources, including the JobParameters file from the previous job step
        Dim result = GetSharedResources()
        If result <> CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        ' Retrieve the parameter file
        Dim strParamFileName As String = m_jobParams.GetParam("ParmFileName")
        Dim strParamFileStoragePath As String = m_jobParams.GetParam("ParmFileStoragePath")

        If Not RetrieveFile(strParamFileName, strParamFileStoragePath) Then
            Return CloseOutType.CLOSEOUT_NO_PARAM_FILE
        End If

        ' Retrieve the PHRP files (for the X!Tandem, Sequest, or MSGF+ source job)
        If Not RetrievePHRPFiles() Then
            Return CloseOutType.CLOSEOUT_FILE_NOT_FOUND
        End If

        ' Retrieve the MASIC ScanStats.txt, ScanStatsEx.txt, and _SICstats.txt files
        If Not RetrieveMASICFiles() Then
            Return CloseOutType.CLOSEOUT_FILE_NOT_FOUND
        End If

        ' In use from June 2013 through November 12, 2015
        ' Retrieve the LLRC .RData files
        'If Not RetrieveLLRCFiles() Then
        '    Return CloseOutType.CLOSEOUT_FILE_NOT_FOUND
        'End If

        If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        Return CloseOutType.CLOSEOUT_SUCCESS

    End Function

    <Obsolete("No longer used")>
    Private Function RetrieveLLRCFiles() As Boolean

        If Not clsAnalysisToolRunnerSMAQC.LLRC_ENABLED Then Throw New Exception("LLRC is disabled -- do not call this function")

        Dim strLLRCRunnerProgLoc As String = m_mgrParams.GetParam("LLRCRunnerProgLoc", "\\gigasax\DMS_Programs\LLRCRunner")
        Dim lstFilesToCopy = New List(Of String)

        lstFilesToCopy.Add(LLRC.LLRCWrapper.RDATA_FILE_ALLDATA)
        lstFilesToCopy.Add(LLRC.LLRCWrapper.RDATA_FILE_MODELS)

        For Each strFileName As String In lstFilesToCopy
            Dim fiSourceFile = New FileInfo(Path.Combine(strLLRCRunnerProgLoc, strFileName))

            If Not fiSourceFile.Exists Then
                m_message = "LLRC RData file not found: " & fiSourceFile.FullName
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return False
            Else
                fiSourceFile.CopyTo(Path.Combine(m_WorkingDir, fiSourceFile.Name))
                m_jobParams.AddResultFileToSkip(fiSourceFile.Name)
            End If

        Next

        Return True

    End Function

    Private Function RetrieveMASICFiles() As Boolean

        Dim createStoragePathInfoFile = False

        Dim strMASICResultsFolderName As String = m_jobParams.GetParam("MASIC_Results_Folder_Name")

        m_jobParams.AddResultFileExtensionToSkip(SCAN_STATS_FILE_SUFFIX)        ' _ScanStats.txt
        m_jobParams.AddResultFileExtensionToSkip(SCAN_STATS_EX_FILE_SUFFIX)     ' _ScanStatsEx.txt
        m_jobParams.AddResultFileExtensionToSkip("_SICstats.txt")
        m_jobParams.AddResultFileExtensionToSkip(REPORTERIONS_FILE_SUFFIX)

        Dim lstNonCriticalFileSuffixes As List(Of String)
        lstNonCriticalFileSuffixes = New List(Of String) From {SCAN_STATS_EX_FILE_SUFFIX, REPORTERIONS_FILE_SUFFIX}

        If String.IsNullOrEmpty(strMASICResultsFolderName) Then
            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Retrieving the MASIC files by searching for any valid MASIC folder")
            End If

            Return RetrieveScanAndSICStatsFiles(
              retrieveSICStatsFile:=True,
              createStoragePathInfoOnly:=createStoragePathInfoFile,
              retrieveScanStatsFile:=True,
              retrieveScanStatsExFile:=True,
              retrieveReporterIonsFile:=True,
              lstNonCriticalFileSuffixes:=lstNonCriticalFileSuffixes)

        Else
            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Retrieving the MASIC files from " & strMASICResultsFolderName)
            End If

            Dim ServerPath As String
            ServerPath = FindValidFolder(m_DatasetName, "", strMASICResultsFolderName, 2)

            If String.IsNullOrEmpty(ServerPath) Then
                m_message = "Dataset folder path not defined"
            Else

                If ServerPath.StartsWith(MYEMSL_PATH_FLAG) Then

                    Dim bestSICFolderPath = Path.Combine(MYEMSL_PATH_FLAG, strMASICResultsFolderName)

                    Return RetrieveScanAndSICStatsFiles(
                      bestSICFolderPath,
                      retrieveSICStatsFile:=True,
                      createStoragePathInfoOnly:=createStoragePathInfoFile,
                      retrieveScanStatsFile:=True,
                      retrieveScanStatsExFile:=True,
                      retrieveReporterIonsFile:=True,
                      lstNonCriticalFileSuffixes:=lstNonCriticalFileSuffixes)
                End If

                Dim diFolderInfo As DirectoryInfo
                Dim diMASICFolderInfo As DirectoryInfo
                diFolderInfo = New DirectoryInfo(ServerPath)

                If Not diFolderInfo.Exists Then
                    m_message = "Dataset folder not found: " & diFolderInfo.FullName
                Else

                    'See if the ServerPath folder actually contains a subfolder named strMASICResultsFolderName
                    diMASICFolderInfo = New DirectoryInfo(Path.Combine(diFolderInfo.FullName, strMASICResultsFolderName))

                    If Not diMASICFolderInfo.Exists Then
                        m_message = "Unable to find MASIC results folder " & strMASICResultsFolderName
                    Else

                        Return RetrieveScanAndSICStatsFiles(
                          diMASICFolderInfo.FullName,
                          retrieveSICStatsFile:=True,
                          createStoragePathInfoOnly:=createStoragePathInfoFile,
                          retrieveScanStatsFile:=True,
                          retrieveScanStatsExFile:=True,
                          retrieveReporterIonsFile:=True,
                          lstNonCriticalFileSuffixes:=lstNonCriticalFileSuffixes)

                    End If
                End If
            End If
        End If

        Return False

    End Function

    Private Function RetrievePHRPFiles() As Boolean

        Dim lstFileNamesToGet As New List(Of String)
        Dim ePeptideHitResultType As clsPHRPReader.ePeptideHitResultType

        ' The Input_Folder for this job step should have been auto-defined by the DMS_Pipeline database using the Special_Processing parameters
        ' For example, for dataset QC_Shew_10_07_pt5_1_21Sep10_Earth_10-07-45 using Special_Processing of
        '   SourceJob:Auto{Tool = "XTandem" AND Settings_File = "IonTrapDefSettings.xml" AND [Parm File] = "xtandem_Rnd1PartTryp.xml"}
        ' leads to the input folder being XTM201009211859_Auto625059

        Dim strInputFolder As String
        strInputFolder = m_jobParams.GetParam("StepParameters", "InputFolderName")

        If String.IsNullOrEmpty(strInputFolder) Then
            m_message = "InputFolder step parameter not found; this is unexpected"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            Return False

        End If

        If strInputFolder.ToUpper().StartsWith("XTM") Then
            ePeptideHitResultType = clsPHRPReader.ePeptideHitResultType.XTandem

        ElseIf strInputFolder.ToUpper().StartsWith("SEQ") Then
            ePeptideHitResultType = clsPHRPReader.ePeptideHitResultType.Sequest

        ElseIf strInputFolder.ToUpper().StartsWith("MSG") Then
            ePeptideHitResultType = clsPHRPReader.ePeptideHitResultType.MSGFDB

        Else
            m_message = "InputFolder is not an X!Tandem, Sequest, or MSGF+ folder; it should start with XTM, Seq, or MSG and is auto-determined by the SourceJob SpecialProcessing text"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            Return False
        End If

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Retrieving the PHRP files")
        End If


        Dim strSynopsisFileName As String

        strSynopsisFileName = clsPHRPReader.GetPHRPSynopsisFileName(ePeptideHitResultType, m_DatasetName)
        lstFileNamesToGet.Add(strSynopsisFileName)

        lstFileNamesToGet.Add(clsPHRPReader.GetPHRPResultToSeqMapFileName(ePeptideHitResultType, m_DatasetName))
        lstFileNamesToGet.Add(clsPHRPReader.GetPHRPSeqInfoFileName(ePeptideHitResultType, m_DatasetName))
        lstFileNamesToGet.Add(clsPHRPReader.GetPHRPSeqToProteinMapFileName(ePeptideHitResultType, m_DatasetName))
        lstFileNamesToGet.Add(clsPHRPReader.GetPHRPModSummaryFileName(ePeptideHitResultType, m_DatasetName))
        lstFileNamesToGet.Add(clsPHRPReader.GetMSGFFileName(strSynopsisFileName))

        For Each FileToGet As String In lstFileNamesToGet

            If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
                'Errors were reported in function call, so just return
                Return False
            End If
            m_jobParams.AddResultFileToSkip(FileToGet)

        Next

        Return True

    End Function

End Class
