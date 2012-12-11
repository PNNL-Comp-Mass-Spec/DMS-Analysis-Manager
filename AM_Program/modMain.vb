Option Strict On

' The DMS Analysis Manager program runs automated data analysis for PRISM
'
' -------------------------------------------------------------------------------
' Written by Dave Clark, Matthew Monroe, and John Sandoval for the Department of Energy (PNNL, Richland, WA)
'
' E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com
' Website: http://ncrr.pnnl.gov/ or http://www.sysbio.org/resources/staff/
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

Module modMain
	Public Const PROGRAM_DATE As String = "December 10, 2012"

	Private mCodeTestMode As Boolean
	Private mCreateWindowsEventLog As Boolean
	Private mTraceMode As Boolean

	Public Function Main() As Integer
		' Returns 0 if no error, error code if an error

		Dim objDMSMain As AnalysisManagerProg.clsMainProcess

		Dim intReturnCode As Integer
		Dim objParseCommandLine As New clsParseCommandLine
		Dim blnProceed As Boolean

		intReturnCode = 0
		mCodeTestMode = False
		mTraceMode = False

		Try
			blnProceed = False

			' Look for /T or /Test on the command line
			' If present, this means "code test mode" is enabled
			' 
			' Other valid switches are /I, /T, /Test, /Trace, /EL, /Q, and /?
			'
			If objParseCommandLine.ParseCommandLine Then
				If SetOptionsUsingCommandLineParameters(objParseCommandLine) Then blnProceed = True
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

					Catch ex As Exception
						Console.WriteLine(AnalysisManagerBase.clsGlobal.GetExceptionStackTrace(ex))
					End Try
				ElseIf mCreateWindowsEventLog Then
					AnalysisManagerProg.clsMainProcess.CreateAnalysisManagerEventLog()
				Else
					' Initiate automated analysis
					If mTraceMode Then ShowTraceMessage("Instantiating clsMainProcess")
					objDMSMain = New AnalysisManagerProg.clsMainProcess(mTraceMode)
					objDMSMain.Main()
					intReturnCode = 0

				End If


			End If

		Catch ex As Exception
			Console.WriteLine("Error occurred in modMain->Main: " & ControlChars.NewLine & ex.Message)
			intReturnCode = -1
		End Try

		Return intReturnCode

	End Function

	Private Function SetOptionsUsingCommandLineParameters(ByVal objParseCommandLine As clsParseCommandLine) As Boolean
		' Returns True if no problems; otherwise, returns false

		Dim strValue As String = String.Empty
		Dim strValidParameters() As String = New String() {"T", "Test", "Trace", "EL"}

		Try
			' Make sure no invalid parameters are present
			If objParseCommandLine.InvalidParametersPresent(strValidParameters) Then
				Return False
			Else
				With objParseCommandLine
					' Query objParseCommandLine to see if various parameters are present

					If .RetrieveValueForParameter("T", strValue) Then mCodeTestMode = True
					If .RetrieveValueForParameter("Test", strValue) Then mCodeTestMode = True

					If .RetrieveValueForParameter("Trace", strValue) Then mTraceMode = True

					If .RetrieveValueForParameter("EL", strValue) Then mCreateWindowsEventLog = True

				End With

				Return True
			End If

		Catch ex As Exception
			Console.WriteLine("Error parsing the command line parameters: " & ControlChars.NewLine & ex.Message)
		End Try

	End Function

	Private Sub ShowProgramHelp()

		Try

			Console.WriteLine("This program processes DMS analysis jobs for PRISM. Normal operation is to run the program without any command line switches.")
			Console.WriteLine()
			Console.WriteLine("Program syntax:" & ControlChars.NewLine & System.IO.Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().Location) & " [/EL] [/T] [/Trace] [/Q]")
			Console.WriteLine()

			Console.WriteLine("Use /EL to create the Windows Event Log named '" & AnalysisManagerProg.clsMainProcess.CUSTOM_LOG_NAME & "' then exit the program.  You should do this from a Windows Command Prompt that you started using 'Run as Administrator'")
			Console.WriteLine()
			Console.WriteLine("Use /T or /Test to start the program in code test mode.")
			Console.WriteLine()
			Console.WriteLine("Use /Trace to enable trace mode, where debug messages are written to the command prompt")
			Console.WriteLine()

			Console.WriteLine("Program written by Dave Clark, Matthew Monroe, and John Sandoval for the Department of Energy (PNNL, Richland, WA)")
			Console.WriteLine()

			Console.WriteLine("This is version " & System.Windows.Forms.Application.ProductVersion & " (" & PROGRAM_DATE & ")")
			Console.WriteLine()

			Console.WriteLine("E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com")
			Console.WriteLine("Website: http://ncrr.pnnl.gov/ or http://www.sysbio.org/resources/staff/")
			Console.WriteLine()

			Console.WriteLine("Licensed under the Apache License, Version 2.0; you may not use this file except in compliance with the License.  " & _
							  "You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0")
			Console.WriteLine()

			Console.WriteLine("Notice: This computer software was prepared by Battelle Memorial Institute, " & _
							  "hereinafter the Contractor, under Contract No. DE-AC05-76RL0 1830 with the " & _
							  "Department of Energy (DOE).  All rights in the computer software are reserved " & _
							  "by DOE on behalf of the United States Government and the Contractor as " & _
							  "provided in the Contract.  NEITHER THE GOVERNMENT NOR THE CONTRACTOR MAKES ANY " & _
							  "WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY FOR THE USE OF THIS " & _
							  "SOFTWARE.  This notice including this sentence must appear on any copies of " & _
							  "this computer software.")

			' Delay for 750 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
			System.Threading.Thread.Sleep(750)

		Catch ex As Exception
			Console.WriteLine("Error displaying the program syntax: " & ex.Message)
		End Try

	End Sub

	Public Sub ShowTraceMessage(ByVal strMessage As String)
		AnalysisManagerProg.clsMainProcess.ShowTraceMessage(strMessage)
	End Sub
End Module




