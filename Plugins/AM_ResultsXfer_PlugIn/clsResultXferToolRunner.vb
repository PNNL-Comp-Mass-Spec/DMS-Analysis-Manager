'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2008, Battelle Memorial Institute
' Created 10/30/2008
'
' Last modified 10/31/2008
' Last modified 06/15/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Imports AnalysisManagerBase

Public Class clsResultXferToolRunner
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Derived class for performing analysis results transfer
	'*********************************************************************************************************

#Region "Constants"
#End Region

#Region "Module variables"
#End Region

#Region "Events"
#End Region

#Region "Properties"
#End Region

#Region "Methods"
	''' <summary>
	''' Runs the results transfer tool
	''' </summary>
	''' <returns>IJobParams.CloseOutType indicating success or failure</returns>
	''' <remarks></remarks>
	Public Overrides Function RunTool() As IJobParams.CloseOutType

		Dim Msg As String = ""
		Dim Result As IJobParams.CloseOutType

		'Call base class for initial setup
		MyBase.RunTool()

        ' Store the AnalysisManager version info in the database
		If Not StoreToolVersionInfo() Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
			m_message = "Error determining AnalysisManager version"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

        ' Transfer the results
		Result = PerformResultsXfer()
		If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			'TODO: Handle any errors
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Stop the job timer
		m_StopTime = System.DateTime.UtcNow

        ' If there are no more folders in the dataset folder in the xfer directory, then delete the folder
        ' Note that another manager might be simultaneously examining this folder to see if it's empty
        ' If that manager deletes this folder first, then an exception could occur in this manager
        ' Thus, we will log any exceptions that occur, but we won't treat them as a job failure

        Try
			Dim DSFolderPath As String = System.IO.Path.Combine(m_jobParams.GetParam("transferFolderPath"), m_jobParams.GetParam("DatasetNum"))
			Dim FoundFolders() As String = System.IO.Directory.GetDirectories(DSFolderPath)

            If FoundFolders.Count = 0 Then
                ' Dataset folder in transfer folder is empty; delete it
                Try
                    If m_DebugLevel >= 3 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting empty dataset folder in transfer directory: " & DSFolderPath)
                    End If

                    System.IO.Directory.Delete(DSFolderPath)
                Catch ex As Exception
                    ' Log this exception, but don't treat it is a job failure
                    Msg = "clsResultXferToolRunner.RunTool(); Exception deleting dataset folder " & _
                      m_jobParams.GetParam("DatasetFolderName") & " in xfer folder(another results manager may have deleted it): " & ex.Message
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, Msg)
                End Try
            Else
                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Dataset folder in transfer directory still has subfolders; will not delete folder: " & DSFolderPath)
                End If
            End If

        Catch ex As Exception
            ' Log this exception, but don't treat it is a job failure
            Msg = "clsResultXferToolRunner.RunTool(); Exception looking for dataset folder " & _
              m_jobParams.GetParam("DatasetFolderName") & " in xfer folder (another results manager may have deleted it): " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, Msg)
        End Try

        'If we got to here, everything worked, so exit
		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
	''' Performs the results transfer
	''' </summary>
	''' <returns>IJobParams.CloseOutType indicating success or failure></returns>
	''' <remarks></remarks>
	Protected Overridable Function PerformResultsXfer() As IJobParams.CloseOutType

		Dim Msg As String
		Dim FolderToMove As String
		Dim DatasetDir As String
		Dim TargetDir As String
        Dim diDatasetFolder As System.IO.DirectoryInfo

        ' Set this to True to overwrite existing results folders
        Dim blnOverwriteExisting As Boolean = True

		'Verify input folder exists in storage server xfer folder
        FolderToMove = System.IO.Path.Combine(m_jobParams.GetParam("transferFolderPath"), m_jobParams.GetParam("DatasetFolderName"))
        FolderToMove = System.IO.Path.Combine(FolderToMove, m_jobParams.GetParam("InputFolderName"))
        If Not System.IO.Directory.Exists(FolderToMove) Then
            Msg = "clsResultXferToolRunner.PerformResultsXfer(); results folder " & FolderToMove & " not found"
            m_message = clsGlobal.AppendToComment(m_message, "results folder not found")
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        ElseIf m_DebugLevel >= 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Results folder to move: " & FolderToMove)
        End If

        ' Verify dataset folder exists on storage server
        ' If it doesn't exist, we will auto-create it (this behavior was added 4/24/2009)
        DatasetDir = System.IO.Path.Combine(m_jobParams.GetParam("DatasetStoragePath"), m_jobParams.GetParam("DatasetFolderName"))
        diDatasetFolder = New System.IO.DirectoryInfo(DatasetDir)
        If Not diDatasetFolder.Exists Then
            Msg = "clsResultXferToolRunner.PerformResultsXfer(); dataset folder " & DatasetDir & " not found; will attempt to make it"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, Msg)

            Try
                Dim diParentFolder As System.IO.DirectoryInfo
                diParentFolder = diDatasetFolder.Parent

                If Not diParentFolder.Exists Then
                    ' Parent folder doesn't exist; try to go up one more level and create the parent

                    If Not diParentFolder.Parent Is Nothing Then
                        ' Parent of the parent exist; try to create the parent folder
                        diParentFolder.Create()

                        ' Wait 500 msec then verify that the folder was created
                        System.Threading.Thread.Sleep(500)
                        diParentFolder.Refresh()
                        diDatasetFolder.Refresh()
                    End If
                End If

                If diParentFolder.Exists Then
                    ' Parent folder exists; try to create the dataset folder
                    diDatasetFolder.Create()

                    ' Wait 500 msec then verify that the folder now exists
                    System.Threading.Thread.Sleep(500)
                    diDatasetFolder.Refresh()

                    If Not diDatasetFolder.Exists Then
                        ' Creation of the dataset folder failed; unable to continue
                        Msg = "clsResultXferToolRunner.PerformResultsXfer(); error trying to create missing dataset folder " & DatasetDir & ": folder creation failed for unknown reason"
                        m_message = clsGlobal.AppendToComment(m_message, "error trying to create missing dataset folder")
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
                        Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                    End If
                Else
                    Msg = "clsResultXferToolRunner.PerformResultsXfer(); parent folder not found: " & diDatasetFolder.Parent.FullName & "; unable to continue"
                    m_message = clsGlobal.AppendToComment(m_message, "parent folder not found: " & diDatasetFolder.Parent.FullName)
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If

            Catch ex As Exception
                Msg = "clsResultXferToolRunner.PerformResultsXfer(); error trying to create missing dataset folder " & DatasetDir & ": " & ex.Message
                m_message = clsGlobal.AppendToComment(m_message, "exception trying to create missing dataset folder")
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)

                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End Try


        ElseIf m_DebugLevel >= 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Dataset folder path: " & DatasetDir)
        End If

        TargetDir = System.IO.Path.Combine(DatasetDir, m_jobParams.GetParam("inputfoldername"))


        'Determine if output folder already exists on storage server
        If System.IO.Directory.Exists(TargetDir) Then
            If blnOverwriteExisting Then
                Msg = "Warning: overwriting existing results folder: " & TargetDir
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, Msg)
                Msg = String.Empty
            Else
                Msg = "clsResultXferToolRunner.PerformResultsXfer(); destination directory " & DatasetDir & " already exists"
                m_message = clsGlobal.AppendToComment(m_message, "results folder already exists at destination and overwrite is disabled")
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
        End If

        'Move the directory
        Try
            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Moving '" & FolderToMove & "' to '" & TargetDir & "'")
            End If

            My.Computer.FileSystem.MoveDirectory(FolderToMove, TargetDir, blnOverwriteExisting)

        Catch ex As Exception
            Msg = "clsResultXferToolRunner.PerformResultsXfer(); Exception moving results folder " & FolderToMove & ": " & ex.Message
            m_message = clsGlobal.AppendToComment(m_message, "exception moving results folder")
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

    End Function

    ''' <summary>
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <remarks></remarks>
    Protected Function StoreToolVersionInfo() As Boolean

        Dim strToolVersionInfo As String = String.Empty
        Dim ioAppFileInfo As System.IO.FileInfo = New System.IO.FileInfo(System.Reflection.Assembly.GetExecutingAssembly().Location)

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
        End If

        ' Lookup the version of the Analysis Manager
        Try
            Dim oAssemblyName As System.Reflection.AssemblyName
            oAssemblyName = System.Reflection.Assembly.Load("AnalysisManagerProg").GetName

            Dim strNameAndVersion As String
            strNameAndVersion = oAssemblyName.Name & ", Version=" & oAssemblyName.Version.ToString()
            strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion)

        Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception determining Assembly info for AnalysisManagerProg: " & ex.Message)
			Return False
        End Try

        ' Lookup the version of AnalysisManagerResultsXferPlugin
        Try
            Dim oAssemblyName As System.Reflection.AssemblyName
            oAssemblyName = System.Reflection.Assembly.Load("AnalysisManagerResultsXferPlugin").GetName

            Dim strNameAndVersion As String
            strNameAndVersion = oAssemblyName.Name & ", Version=" & oAssemblyName.Version.ToString()
            strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion)

        Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception determining Assembly info for AnalysisManagerResultsXferPlugin: " & ex.Message)
			Return False
        End Try

        ' Store the path to AnalysisManagerProg.exe and AnalysisManagerResultsXferPlugin.dll in ioToolFiles
        Dim ioToolFiles As New System.Collections.Generic.List(Of System.IO.FileInfo)
        ioToolFiles.Add(New System.IO.FileInfo(System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "AnalysisManagerProg.exe")))
        ioToolFiles.Add(New System.IO.FileInfo(System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "AnalysisManagerResultsXferPlugin.dll")))

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, False)
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function

#End Region

End Class
