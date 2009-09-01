'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 10/20/2006
'
' Last modified 10/26/2006
'*********************************************************************************************************

Imports System.Runtime.Remoting
Imports System.Runtime.Remoting.Channels
Imports System.Runtime.Remoting.Channels.Tcp
Imports Decon2LSRemoter
Imports System.IO

Module clsDecon2LSCAOServer

	'*********************************************************************************************************
	'Server for running Decon2LS in a process separate from the main Decon2LS plug-in using .Net Remoting. 
	'This is done because the Finnigan library used by Decon2LS locks the raw data file until the process
	'	calling Decon2LS exits. Remoting is not necessary to run Decon2SL separately, but it allows inter-process
	'	communication between the rest of the plug-in and Decon2LS itself.
	'
	'The server is configured as a Client Activated Object (CAO), meaning its lifetime is directly controlled by the
	'	client. Using a CAO is simpler for this application than a Server Activated Object because there will
	'	only be one client and one server for each analysis manager instance
	'
	'Remoting is done via TCP over port specified in manager setup file.
	'*********************************************************************************************************

#Region "Constants"
	'	Const SVR_CHAN As Integer = 54321
	Const FLAG_FILE_NAME As String = "FlagFile_Svr.txt"
#End Region

#Region "Module variables"
	Dim WithEvents m_FlagFileWatcher As FileSystemWatcher
	Dim m_FlagFilePath As String = ""
	Dim m_KeepRunning As Boolean = True
	Dim WithEvents m_DisplayTmr As System.Timers.Timer
	Dim m_TcpPort As Integer = 0
#End Region

#Region "Methods"
	Sub Main()

		'Read the command line argurments
		Dim ResCode As Integer = GetCmdArgs()
		If ResCode < 0 Then
			Environment.Exit(ResCode)
		End If

		'Create and register a TCP channel
		Dim Channel As TcpServerChannel = New TcpServerChannel(m_TcpPort)
		ChannelServices.RegisterChannel(Channel, False)

		'Register the client activated object
		RemotingConfiguration.RegisterActivatedServiceType(GetType(clsDecon2LSRemoter))

		System.Console.WriteLine("Activated Decon2LS remoting service")

		'Create a flag file that will be used to control server lifetime
		Dim RetVal As Integer = CreateFlagFile()
		If RetVal <> 0 Then
			Environment.Exit(RetVal)
		End If

		'Initialize a file watcher to monitor the flag file
		InitFileWatcher()

		'Initialze a timer for the "I'm alive" message
		m_DisplayTmr = New System.Timers.Timer
		m_DisplayTmr.AutoReset = True
		m_DisplayTmr.Interval = 30000		'30 seconds
		m_DisplayTmr.Enabled = True

		'Go into a loop and wait around until the file watcher detects that the flag file has been deleted
		While m_KeepRunning
			System.Threading.Thread.Sleep(1000)			'Pause 1 second
		End While

		'The loop has ended, so exit gracefully
		m_DisplayTmr.Enabled = False
		System.Console.WriteLine("Shutting down service")
		Environment.Exit(0)		 'Inidicates normal exit from server

	End Sub

	Private Function CreateFlagFile() As Integer

		'Creates a flag file that will be used to tell server it's time to quit
		'Returns 0 for success; -10 for error

		' Create an instance of StreamWriter to write text to a file.
		Try
			Dim sw As StreamWriter = New StreamWriter(Path.Combine(m_FlagFilePath, FLAG_FILE_NAME), False)
			sw.WriteLine(Now.ToString)
			sw.Close()
			Return 0
		Catch ex As Exception
			Return -10
		End Try

	End Function

	'Private Function GetOutputPath() As String

	'	'DANGER: Assumes that output path is ALWAYS specified as 1st command line parameter. No error if not specifed
	'	Try
	'		Return Environment.GetCommandLineArgs(1)
	'	Catch ex As Exception
	'		Return ""
	'	End Try

	'End Function

	Private Sub InitFileWatcher()

		'Initializes a file watcher. The filewatcher monitors the flag file created in CreateFlagFile. When the flag file
		'is erased by another program, this server closes
		m_FlagFileWatcher = New FileSystemWatcher(m_FlagFilePath)
		With m_FlagFileWatcher
			.BeginInit()
			.Filter = "*.txt"
			.IncludeSubdirectories = False
			.EndInit()
		End With
		m_FlagFileWatcher.EnableRaisingEvents = True

	End Sub

	Private Sub m_FlagFileWatcher_Deleted(ByVal sender As Object, ByVal e As System.IO.FileSystemEventArgs) Handles m_FlagFileWatcher.Deleted

		m_FlagFileWatcher.EnableRaisingEvents = False		 'Prevent any other events from messing up the logic
		m_KeepRunning = False		 'Causes the wait loop to exit next iteration

	End Sub

	Private Sub m_DisplayTmr_Elapsed(ByVal sender As Object, ByVal e As System.Timers.ElapsedEventArgs) Handles m_DisplayTmr.Elapsed

		'Output an "I'm alive" message to the console
		System.Console.WriteLine(Now.ToString & ", CAOServer running")

	End Sub

	Private Function GetCmdArgs() As Integer

		'Parses the command line to obtain the tcp port and output path for the flag file.
		'Returns values:
		'	0 = success
		'	-1 = generic error
		'	-2 = invalid argument count
		'	-3 = TCP port spec problem
		'	-4 = output path problem

		Dim CmdArgs() As String = Environment.GetCommandLineArgs

        If CmdArgs.Length <> 3 Then
            ' We should have 3 arguments in CmdArgs (the first "argument" is the program name)
            If CmdArgs.Length <= 1 Then
                ShowSyntax()
            Else
                ShowSyntax("Invalid number of arguments found at the command line;" & ControlChars.NewLine & " expecting 2 but found " & (CmdArgs.Length - 1).ToString)
            End If
            Return -2 'Invalid number of arguments
        End If


		'Fill module variables from command line argurments
		For Each CmdArg As String In CmdArgs
			Try
				Select Case CmdArg.ToLower.Substring(0, 2)
					Case "-p"
						Try
							m_TcpPort = CInt(CmdArg.Remove(0, 2))
						Catch ex As Exception
                            ShowSyntax("Exception defining the TcpPort using the -p switch: " & ControlChars.NewLine & ex.Message)
                            Return -3
						End Try
					Case "-o"
						Try
							m_FlagFilePath = CmdArg.Remove(0, 2)
						Catch ex As Exception
                            ShowSyntax("Exception defining the Flag File Folder Path using the -o switch: " & ControlChars.NewLine & ex.Message)
                            Return -4
						End Try
					Case Else
						'Do nothing - this is the program name argument
				End Select
            Catch ex As Exception
                ShowSyntax("Exception parsing command line parameters: " & ControlChars.NewLine & ex.Message)
                Return -1
			End Try
		Next

		'Verify that all necessary module variables have been found
        If m_FlagFilePath = "" Then
            ShowSyntax("Flag file path is not defined (use the -o switch); unable to continue")
            Return -1
        ElseIf m_TcpPort = 0 Then
            ShowSyntax("TCP Port is not defined (use the -p switch); unable to continue")
            Return -1
        Else
            Return 0
        End If

    End Function

    Private Sub ShowSyntax()
        ShowSyntax(String.Empty)
    End Sub

    Private Sub ShowSyntax(ByVal strErrorMessage As String)

        If strErrorMessage Is Nothing Then strErrorMessage = String.Empty

        If strErrorMessage.Length > 0 Then
            Console.WriteLine("Error: " & strErrorMessage)
        End If
        Console.WriteLine()

        Console.WriteLine("  Syntax: Decon2LSCAOServer.exe -oFlagFileFolderPath -pTCPPort")
        Console.WriteLine("  Example command: Decon2LSCAOServer.exe -oC:\DMS_Programs -p54321")
    End Sub

#End Region

End Module
