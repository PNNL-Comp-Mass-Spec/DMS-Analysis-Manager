'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 03/30/2011
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.IO
Imports System.Runtime.InteropServices

Public Class clsAnalysisToolRunnerMSXMLBruker
    Inherits clsAnalysisToolRunnerBase

#Region "Module Variables"
    Protected Const PROGRESS_PCT_MSXML_GEN_RUNNING As Single = 5

    Protected Const COMPASS_XPORT As String = "CompassXport.exe"

    Protected mMSXmlCacheFolder As DirectoryInfo

    Protected WithEvents mCompassXportRunner As clsCompassXportRunner

#End Region

#Region "Methods"

	Protected Const MAX_CSV_FILES As Integer = 50

	''' <summary>
	''' Constructor
	''' </summary>
	''' <remarks>Presently not used</remarks>
	Public Sub New()

	End Sub

	''' <summary>
	''' Runs ReadW tool
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Public Overrides Function RunTool() As IJobParams.CloseOutType
		Dim eResult As IJobParams.CloseOutType
		Dim eReturnCode As IJobParams.CloseOutType

		' Set this to success for now
		eReturnCode = IJobParams.CloseOutType.CLOSEOUT_SUCCESS

		'Do the base class stuff
		If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Store the CompassXport version info in the database
		If Not StoreToolVersionInfo() Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
			If String.IsNullOrEmpty(m_message) Then
				m_message = "Error determining CompassXport version"
			End If
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

        Dim msXMLCacheFolderPath As String = m_mgrParams.GetParam("MSXMLCacheFolderPath", String.Empty)
        mMSXmlCacheFolder = New DirectoryInfo(msXMLCacheFolderPath)

        If Not mMSXmlCacheFolder.Exists Then
            LogError("MSXmlCache folder not found: " & msXMLCacheFolderPath)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

		Dim processingErrorMessage As String = String.empty
        Dim fiResultsFile As FileInfo = Nothing

        eResult = CreateMSXmlFile(fiResultsFile)

		If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			' Something went wrong
			' In order to help diagnose things, we will move whatever files were created into the eResult folder, 
			'  archive it using CopyFailedResultsToArchiveFolder, then return IJobParams.CloseOutType.CLOSEOUT_FAILED
			If String.IsNullOrEmpty(m_message) Then
				m_message = "Error running CompassXport"
			End If
			processingErrorMessage = String.copy(m_message)

			If eResult = IJobParams.CloseOutType.CLOSEOUT_NO_DATA Then
				eReturnCode = eResult
			Else
				eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If
        Else
            ' Gzip the .mzML or .mzXML file then copy to the server cache
            eReturnCode = PostProcessMsXmlFile(fiResultsFile)
        End If

        'Stop the job timer
        m_StopTime = DateTime.UtcNow

        'Delete the raw data files
        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMSXMLBruker.RunTool(), Deleting raw data file")
        End If

        If DeleteRawDataFiles() <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerMSXMLBruker.RunTool(), Problem deleting raw data files: " & m_message)

            If Not String.IsNullOrEmpty(processingErrorMessage) Then
                m_message = processingErrorMessage
            Else
                ' Don't treat this as a critical error; leave eReturnCode unchanged
                m_message = "Error deleting raw data files"
            End If
        End If

        'Update the job summary file
        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMSXMLBruker.RunTool(), Updating summary file")
        End If
        UpdateSummaryFile()

        'Make the results folder
        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMSXMLBruker.RunTool(), Making results folder")
        End If

        eResult = MakeResultsFolder()
        If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            ' MakeResultsFolder handles posting to local log, so set database error message and exit
            m_message = "Error making results folder"
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        eResult = MoveResultFiles()
        If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            ' MoveResultFiles moves the eResult files to the eResult folder
            m_message = "Error moving files into results folder"
            eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        If eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED Then
            ' Try to save whatever files were moved into the results folder
            Dim objAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
            objAnalysisResults.CopyFailedResultsToArchiveFolder(Path.Combine(m_WorkDir, m_ResFolderName))

            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        eResult = CopyResultsFolderToServer()
        If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return eResult
        End If

        'If we get to here, everything worked so exit happily
        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Generate the mzXML or mzML file
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function CreateMSXmlFile(<Out()> ByRef fiResultsFile As FileInfo) As IJobParams.CloseOutType

        If m_DebugLevel > 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMSXMLGen.CreateMSXmlFile(): Enter")
        End If

        Dim msXmlGenerator As String = m_jobParams.GetParam("MSXMLGenerator")           ' Typically CompassXport.exe

        Dim msXmlFormat As String = m_jobParams.GetParam("MSXMLOutputType")             ' Typically mzXML or mzML
        Dim CentroidMSXML = CBool(m_jobParams.GetParam("CentroidMSXML"))

        Dim CompassXportProgramPath As String
        Dim eOutputType As clsCompassXportRunner.MSXMLOutputTypeConstants

        Dim blnSuccess As Boolean

        ' Initialize the Results File output parameter to a dummy name for now
        fiResultsFile = New FileInfo(Path.Combine(m_WorkDir, "NonExistent_Placeholder_File.tmp"))

        If String.Equals(msXmlGenerator, COMPASS_XPORT, StringComparison.CurrentCultureIgnoreCase) Then
            CompassXportProgramPath = m_mgrParams.GetParam("CompassXportLoc")

            If String.IsNullOrEmpty(CompassXportProgramPath) Then
                m_message = "Manager parameter CompassXportLoc is not defined in the Manager Control DB"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            If Not File.Exists(CompassXportProgramPath) Then
                m_message = "CompassXport program not found"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & " at " & CompassXportProgramPath)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
        Else
            m_message = "Invalid value for MSXMLGenerator: " & msXmlGenerator
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        eOutputType = clsCompassXportRunner.GetMsXmlOutputTypeByName(msXmlFormat)
        If eOutputType = clsCompassXportRunner.MSXMLOutputTypeConstants.Invalid Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "msXmlFormat string is not recognized (" & msXmlFormat & "); it is typically mzXML, mzML, or CSV; will default to mzXML")
            eOutputType = clsCompassXportRunner.MSXMLOutputTypeConstants.mzXML
        End If

        fiResultsFile = New FileInfo(Path.Combine(m_WorkDir, m_Dataset & "." & clsCompassXportRunner.GetMsXmlOutputTypeByID(eOutputType)))

        ' Instantiate the processing class
        mCompassXportRunner = New clsCompassXportRunner(m_WorkDir, CompassXportProgramPath, m_Dataset, eOutputType, CentroidMSXML)

        ' Create the file
        blnSuccess = mCompassXportRunner.CreateMSXMLFile

        If Not blnSuccess Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, mCompassXportRunner.ErrorMessage)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        ElseIf mCompassXportRunner.ErrorMessage.Length > 0 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, mCompassXportRunner.ErrorMessage)

        End If

        If eOutputType <> clsCompassXportRunner.MSXMLOutputTypeConstants.CSV Then
            If fiResultsFile.Exists Then
                Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
            End If

            m_message = "MSXml results file not found: " & fiResultsFile.FullName
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
        End If

        ' CompassXport created one CSV file for each spectrum in the dataset
        ' Confirm that fewer than 100 CSV files were created

        Dim diWorkDir As New DirectoryInfo(m_WorkDir)
        Dim fiFiles As List(Of FileInfo) = diWorkDir.GetFiles("*.csv").ToList()

        If fiFiles.Count < MAX_CSV_FILES Then
            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
        End If

        m_message = "CompassXport created " & fiFiles.Count.ToString() & " CSV files. The CSV conversion mode is only appropriate for datasets with fewer than " & MAX_CSV_FILES & " spectra; create a mzXML file instead (e.g., settings file mzXML_Bruker.xml)"
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)

        For Each fiFile In fiFiles
            Try
                fiFile.Delete()
            Catch ex As Exception
                ' Ignore errors here
            End Try
        Next

        Return IJobParams.CloseOutType.CLOSEOUT_FAILED

    End Function

    Private Function PostProcessMsXmlFile(fiResultsFile As FileInfo) As IJobParams.CloseOutType

        ' Compress the file using GZip
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "GZipping " & fiResultsFile.Name)
        fiResultsFile = GZipFile(fiResultsFile)

        If fiResultsFile Is Nothing Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Copy the .mzXML.gz or .mzML.gz file to the MSXML cache
        Dim remoteCachefilePath = CopyFileToServerCache(mMSXmlCacheFolder.FullName, fiResultsFile.FullName, purgeOldFilesIfNeeded:=True)

        If String.IsNullOrEmpty(remoteCachefilePath) Then
            If String.IsNullOrEmpty(m_message) Then
                LogError("CopyFileToServerCache returned false for " & fiResultsFile.Name)
            End If
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Create the _CacheInfo.txt file
        Dim cacheInfoFilePath = fiResultsFile.FullName & "_CacheInfo.txt"
        Using swOutFile = New StreamWriter(New FileStream(cacheInfoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
            swOutFile.WriteLine(remoteCachefilePath)
        End Using

        m_jobParams.AddResultFileToSkip(fiResultsFile.Name)

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <remarks></remarks>
    Protected Function StoreToolVersionInfo() As Boolean

        Dim strToolVersionInfo As String = String.Empty

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
        End If

        ' Store paths to key files in ioToolFiles
        Dim ioToolFiles As New List(Of FileInfo)

        Dim msXmlGenerator As String = m_jobParams.GetParam("MSXMLGenerator")           ' Typically CompassXport.exe
        If String.Equals(msXmlGenerator, COMPASS_XPORT, StringComparison.CurrentCultureIgnoreCase) Then
            Dim compassXportPath = m_mgrParams.GetParam("CompassXportLoc")
            If String.IsNullOrEmpty(compassXportPath) Then
                m_message = "Path defined by manager param CompassXportLoc is empty"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return False
            End If

            Try
                ioToolFiles.Add(New FileInfo(compassXportPath))
            Catch ex As Exception
                m_message = "Path defined by manager param CompassXportLoc is invalid: " + compassXportPath
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + "; " + ex.Message)
                Return False
            End Try

        Else
            If String.IsNullOrEmpty(msXmlGenerator) Then
                m_message = "Job Parameter MSXMLGenerator is not defined"
            Else
                m_message = "Invalid value for MSXMLGenerator, should be " & COMPASS_XPORT & ", not " & msXmlGenerator
            End If

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            Return False
        End If

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function

#End Region

#Region "Event Handlers"
    ''' <summary>
    ''' Event handler for CompassXportRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CompassXportRunner_LoopWaiting() Handles mCompassXportRunner.LoopWaiting

        UpdateStatusFile(PROGRESS_PCT_MSXML_GEN_RUNNING)

        LogProgress("CompassXport")

    End Sub

    ''' <summary>
    ''' Event handler for mCompassXportRunner.ProgRunnerStarting event
    ''' </summary>
    ''' <param name="CommandLine">The command being executed (program path plus command line arguments)</param>
    ''' <remarks></remarks>
    Private Sub mCompassXportRunner_ProgRunnerStarting(CommandLine As String) Handles mCompassXportRunner.ProgRunnerStarting
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, CommandLine)
    End Sub
#End Region

End Class
