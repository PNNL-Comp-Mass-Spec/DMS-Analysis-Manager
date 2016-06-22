Option Strict On

' The DMS Analysis Manager program runs automated data analysis for PRISM
'
' -------------------------------------------------------------------------------
' Written by Dave Clark, Matthew Monroe, and John Sandoval for the Department of Energy (PNNL, Richland, WA)
'
' E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com
' Website: http://omics.pnl.gov/ or http://www.sysbio.org/resources/staff/ or http://panomics.pnnl.gov/
' -------------------------------------------------------------------------------
' 
' Licensed under the Apache License, Version 2.0; you may not use this file except
' in compliance with the License.  You may obtain a copy of the License at 
' http://www.apache.org/licenses/LICENSE-2.0
'
' Notice: This computer software was prepared by Battelle Memorial Institute, 
' hereinafter the Contractor, under Contract No. DE-AC05-76RL0 1830 with the 
' Department of Energy (DOE).  All rights in the computer software are reserved 
' by DOE on behalf of the United States Government and the Contractor as 
' provided in the Contract.  NEITHER THE GOVERNMENT NOR THE CONTRACTOR MAKES ANY 
' WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY FOR THE USE OF THIS 
' SOFTWARE.  This notice including this sentence must appear on any copies of 
' this computer software.

Imports AnalysisManagerBase
Imports System.Reflection
Imports System.IO
Imports System.Threading

Module modMain
    Public Const PROGRAM_DATE As String = "June 22, 2016"

	Private mCodeTestMode As Boolean
	Private mCreateWindowsEventLog As Boolean
	Private mTraceMode As Boolean
    Private mDisableMessageQueue As Boolean
    Private mDisplayDllVersions As Boolean
    Private mDisplayDllPath As String

	Public Function Main() As Integer
		' Returns 0 if no error, error code if an error

        Dim intReturnCode As Integer
		Dim objParseCommandLine As New clsParseCommandLine

		intReturnCode = 0
		mCodeTestMode = False
		mTraceMode = False
        mDisableMessageQueue = False
        mDisplayDllVersions = False
        mDisplayDllPath = ""

		Try

			' Look for /T or /Test on the command line
			' If present, this means "code test mode" is enabled
			' 
            ' Other valid switches are /I, /NoStatus, /T, /Test, /Trace, /EL, /Q, and /?
			'
			If objParseCommandLine.ParseCommandLine Then
				SetOptionsUsingCommandLineParameters(objParseCommandLine)
			End If

			If objParseCommandLine.NeedToShowHelp Then
				ShowProgramHelp()
				intReturnCode = -1
			Else
				If mTraceMode Then ShowTraceMessage("Command line arguments parsed")

				' Note: CodeTestMode is enabled using command line switch /T
				If mCodeTestMode Then

					If mTraceMode Then ShowTraceMessage("Code test mode enabled")

                    Dim objTest As New clsCodeTest
                    objTest.TraceMode = mTraceMode

					Try
						'objTest.TestFileDateConversion()
						'objTest.TestArchiveFileStart()
						'objTest.TestDTASplit()
						'objTest.TestUncat("Cyano_Nitrogenase_BU_1_12Apr12_Earth_12-03-24", "F:\Temp\Deconcat")
						'objTest.TestFileSplitThenCombine()
						'objTest.TestResultsTransfer()
						'objTest.TestDeliverResults()
						'objTest.TestGetFileContents()

						'objTest.FixICR2LSResultFileNames("E:\DMS_WorkDir", "Test")
						'objTest.TestFindAndReplace()

						'objTest.TestProgRunner()
						'objTest.TestUnzip("f:\'temp\QC_Shew_500_100_fr720_c2_Ek_0000_isos.zip", "f:\temp")

						'objTest.CheckETDModeEnabledXTandem("input.xml", False)
						'objTest.TestDTAWatcher("E:\DMS_WorkDir", 5)

						'objTest.TestProteinDBExport("C:\DMS_Temp_Org")

						'objTest.TestFindFile()
						'objTest.TestDeleteFiles()

						'objTest.TestZipAndUnzip()
                        'objTest.TestMALDIDataUnzip("")

						'objTest.TestMSGFResultsSummarizer()

						'objTest.TestProgRunnerIDPicker()

						'objTest.TestProteinDBExport("c:\dms_temp_org")

						'objTest.PerformanceCounterTest()
                        'objTest.SystemMemoryUsage()

						' objTest.TestIonicZipTools()

						'objTest.RemoveSparseSpectra()

						' objTest.ProcessDtaRefineryLogFiles()

						'objTest.TestZip()
						'objTest.TestGZip()

						'objTest.ConvertZipToGZip("F:\Temp\GZip\Diabetes_iPSC_KO2_TMT_NiNTA_04_21Oct13_Pippin_13-06-18_msgfplus.zip")

						'objTest.TestRunQuery()
						'objTest.TestRunSP()

						'objTest.ValidateCentroided()

						'Console.WriteLine(clsGlobal.DecodePassword("Test"))

						'Console.WriteLine(clsGlobal.UpdateHostName("\\winhpcfs\Projects\dms", "\\picfs.pnl.gov\"))

                        'objTest.TestCosoleOutputParsing()
                        ' objTest.TestMSXmlCachePurge()

                        ' Dim testLogger = New PRISM.Logging.clsDBLogger()
                        ' Console.WriteLine(testLogger.MachineName)

                        ' objTest.TestGetVersionInfo()

                        ' objTest.ParseMSPathFinderConsoleOutput()

                        ' objTest.ParseMSGFDBConsoleOutput()

                        ' objTest.RunMSConvert()

                        ' objTest.GetLegacyFastaFileSize()

                        objTest.GenerateScanStatsFile()

					Catch ex As Exception
                        Console.WriteLine(clsGlobal.GetExceptionStackTrace(ex, True))
                    End Try

                    Return 0

				ElseIf mCreateWindowsEventLog Then
                    clsMainProcess.CreateAnalysisManagerEventLog()

                ElseIf mDisplayDllVersions Then
                    Dim objTest As New clsCodeTest
                    objTest.TraceMode = mTraceMode
                    objTest.DisplayDllVersions(mDisplayDllPath)

                Else
                    ' Initiate automated analysis
                    If mTraceMode Then ShowTraceMessage("Instantiating clsMainProcess")

                    Dim objDMSMain = New clsMainProcess(mTraceMode)
                    objDMSMain.DisableMessageQueue = mDisableMessageQueue

                    intReturnCode = objDMSMain.Main()

				End If


			End If

		Catch ex As Exception
            ShowErrorMessage("Error occurred in modMain->Main: " & Environment.NewLine & ex.Message)
            Console.WriteLine(clsGlobal.GetExceptionStackTrace(ex, True))
			intReturnCode = -1
		End Try

		Return intReturnCode

	End Function

	Private Function GetAppPath() As String
		Return Assembly.GetExecutingAssembly().Location
	End Function

	''' <summary>
	''' Returns the .NET assembly version followed by the program date
	''' </summary>
	''' <param name="strProgramDate"></param>
	''' <returns></returns>
	''' <remarks></remarks>
    Private Function GetAppVersion(strProgramDate As String) As String
        Return Assembly.GetExecutingAssembly().GetName().Version.ToString() & " (" & strProgramDate & ")"
    End Function

    Private Function SetOptionsUsingCommandLineParameters(objParseCommandLine As clsParseCommandLine) As Boolean
        ' Returns True if no problems; otherwise, returns false

        Dim strValue As String = String.Empty
        Dim lstValidParameters As List(Of String) = New List(Of String) From {"T", "Test", "Trace", "EL", "NQ", "DLL"}

        Try
            ' Make sure no invalid parameters are present
            If objParseCommandLine.InvalidParametersPresent(lstValidParameters) Then
                ShowErrorMessage("Invalid commmand line parameters",
                  (From item In objParseCommandLine.InvalidParameters(lstValidParameters) Select "/" + item).ToList())
                Return False
            Else
                With objParseCommandLine
                    ' Query objParseCommandLine to see if various parameters are present

                    If .IsParameterPresent("T") Then mCodeTestMode = True
                    If .IsParameterPresent("Test") Then mCodeTestMode = True

                    If .IsParameterPresent("Trace") Then mTraceMode = True

                    If .IsParameterPresent("EL") Then mCreateWindowsEventLog = True

                    If .IsParameterPresent("NQ") Then mDisableMessageQueue = True

                    If .IsParameterPresent("DLL") Then
                        mDisplayDllVersions = True
                        If .RetrieveValueForParameter("DLL", strValue) Then
                            If Not String.IsNullOrWhiteSpace(strValue) Then
                                mDisplayDllPath = strValue
                            End If
                        End If
                    End If
                End With

                Return True
            End If

        Catch ex As Exception
            ShowErrorMessage("Error parsing the command line parameters: " & Environment.NewLine & ex.Message)
        End Try

        Return False

    End Function

    Private Sub ShowErrorMessage(strMessage As String)
        Const strSeparator As String = "------------------------------------------------------------------------------"

        Console.WriteLine()
        Console.WriteLine(strSeparator)
        Console.WriteLine(strMessage)
        Console.WriteLine(strSeparator)
        Console.WriteLine()

        WriteToErrorStream(strMessage)
    End Sub

    Private Sub ShowErrorMessage(strTitle As String, items As IEnumerable(Of String))
        Const strSeparator As String = "------------------------------------------------------------------------------"
        Dim strMessage As String

        Console.WriteLine()
        Console.WriteLine(strSeparator)
        Console.WriteLine(strTitle)
        strMessage = strTitle & ":"

        For Each item As String In items
            Console.WriteLine("   " + item)
            strMessage &= " " & item
        Next
        Console.WriteLine(strSeparator)
        Console.WriteLine()

        WriteToErrorStream(strMessage)
    End Sub

    Private Sub ShowProgramHelp()

        Try

            Console.WriteLine("This program processes DMS analysis jobs for PRISM. Normal operation is to run the program without any command line switches.")
            Console.WriteLine()
            Console.WriteLine("Program syntax:" & ControlChars.NewLine & Path.GetFileName(GetAppPath()) & " [/EL] [/NQ] [/T] [/Trace] [/DLL]")
            Console.WriteLine()

            Console.WriteLine("Use /EL to create the Windows Event Log named '" & clsMainProcess.CUSTOM_LOG_NAME & "' then exit the program.  You should do this from a Windows Command Prompt that you started using 'Run as Administrator'")
            Console.WriteLine()
            Console.WriteLine("Use /NQ to disable posting status messages to the message queue")
            Console.WriteLine()
            Console.WriteLine("Use /T or /Test to start the program in code test mode.")
            Console.WriteLine()
            Console.WriteLine("Use /Trace to enable trace mode, where debug messages are written to the command prompt")
            Console.WriteLine()
            Console.WriteLine("Use /DLL to display the version of all DLLs in the same folder as this .exe")
            Console.WriteLine("Use /DLL:Path to display the version of all DLLs in the specified folder (surround path with double quotes if spaces)")
            Console.WriteLine()

            Console.WriteLine("Program written by Dave Clark, Matthew Monroe, and John Sandoval for the Department of Energy (PNNL, Richland, WA)")
            Console.WriteLine()

            Console.WriteLine("Version: " & GetAppVersion(PROGRAM_DATE))
            Console.WriteLine()

            Console.WriteLine("E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com")
            Console.WriteLine("Website: http://omics.pnl.gov/ or http://panomics.pnnl.gov/")
            Console.WriteLine()

            Console.WriteLine("Licensed under the Apache License, Version 2.0; you may not use this file except in compliance with the License.  " &
                              "You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0")
            Console.WriteLine()

            Console.WriteLine("Notice: This computer software was prepared by Battelle Memorial Institute, " &
                  "hereinafter the Contractor, under Contract No. DE-AC05-76RL0 1830 with the " &
                  "Department of Energy (DOE).  All rights in the computer software are reserved " &
                  "by DOE on behalf of the United States Government and the Contractor as " &
                  "provided in the Contract.  NEITHER THE GOVERNMENT NOR THE CONTRACTOR MAKES ANY " &
                  "WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY FOR THE USE OF THIS " &
                  "SOFTWARE.  This notice including this sentence must appear on any copies of " &
                  "this computer software.")

            ' Delay for 750 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
            Thread.Sleep(750)

        Catch ex As Exception
            ShowErrorMessage("Error displaying the program syntax: " & ex.Message)
        End Try

    End Sub

    Private Sub WriteToErrorStream(strErrorMessage As String)
        Try
            Using swErrorStream As StreamWriter = New StreamWriter(Console.OpenStandardError())
                swErrorStream.WriteLine(strErrorMessage)
            End Using
        Catch ex As Exception
            ' Ignore errors here
        End Try
    End Sub

    Public Sub ShowTraceMessage(strMessage As String)
        clsMainProcess.ShowTraceMessage(strMessage)
    End Sub
End Module




