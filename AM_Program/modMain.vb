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
	Public Const PROGRAM_DATE As String = "April 12, 2012"

	Private mInputFilePath As String

	Private mCodeTestMode As Boolean

	Private mQuietMode As Boolean

	Public Function Main() As Integer
		' Returns 0 if no error, error code if an error

		Dim objDMSMain As AnalysisManagerProg.clsMainProcess

		Dim intReturnCode As Integer
		Dim objParseCommandLine As New clsParseCommandLine
		Dim blnProceed As Boolean

		intReturnCode = 0
		mInputFilePath = String.Empty
		mCodeTestMode = False

		Try
			blnProceed = False

			' Look for /T or /Test on the command line
			' If present, this means "code test mode" is enabled
			If objParseCommandLine.ParseCommandLine Then
				If SetOptionsUsingCommandLineParameters(objParseCommandLine) Then blnProceed = True
			End If

			If objParseCommandLine.NeedToShowHelp Then
				ShowProgramHelp()
				intReturnCode = -1
			Else

				' Note: CodeTestMode is enabled using command line switch /T
				If mCodeTestMode Then

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

						Dim blnSuccess As Boolean
						If Not mInputFilePath Is Nothing AndAlso mInputFilePath.Length > 0 Then
							blnSuccess = objTest.ValidateSequestNodeCount(mInputFilePath, True)
						Else
							blnSuccess = objTest.ValidateSequestNodeCount("f:\temp\sequest.log", True)
							blnSuccess = objTest.ValidateSequestNodeCount("\\proto-9\DMS3_XFER\HMEC_AA_extraction_7pt2ug_031312_r2\Seq201204161109_Auto826662\sequest.log", True)
							blnSuccess = objTest.ValidateSequestNodeCount("\\proto-7\VOrbiETD04\2012_2\HuPlasma_top15_01_Thresh_13Apr12_Cougar_12-03-21\Seq201204161143_Auto826701\sequest.log", True)
						End If

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

					Catch ex As Exception
						Console.WriteLine(AnalysisManagerBase.clsGlobal.GetExceptionStackTrace(ex))
					End Try

				Else
					' Initiate automated analysis
					objDMSMain = New AnalysisManagerProg.clsMainProcess

					objDMSMain.Main()
					intReturnCode = 0

				End If


			End If

		Catch ex As Exception
			If mQuietMode Then
				Throw ex
			Else
				Console.WriteLine("Error occurred in modMain->Main: " & ControlChars.NewLine & ex.Message)
			End If
			intReturnCode = -1
		End Try

		Return intReturnCode

	End Function

	Private Function SetOptionsUsingCommandLineParameters(ByVal objParseCommandLine As clsParseCommandLine) As Boolean
		' Returns True if no problems; otherwise, returns false

		Dim strValue As String = String.Empty
		Dim strValidParameters() As String = New String() {"I", "T", "Test", "Q"}

		Try
			' Make sure no invalid parameters are present
			If objParseCommandLine.InvalidParametersPresent(strValidParameters) Then
				Return False
			Else
				With objParseCommandLine
					' Query objParseCommandLine to see if various parameters are present
					If .RetrieveValueForParameter("I", strValue) Then
						mInputFilePath = strValue
					ElseIf .NonSwitchParameterCount > 0 Then
						mInputFilePath = .RetrieveNonSwitchParameter(0)
					End If

					If .RetrieveValueForParameter("T", strValue) Then mCodeTestMode = True
					If .RetrieveValueForParameter("Test", strValue) Then mCodeTestMode = True

					If .RetrieveValueForParameter("Q", strValue) Then mQuietMode = True
				End With

				Return True
			End If

		Catch ex As Exception
			If mQuietMode Then
				Throw New System.Exception("Error parsing the command line parameters", ex)
			Else
				Console.WriteLine("Error parsing the command line parameters: " & ControlChars.NewLine & ex.Message)
			End If
		End Try

	End Function

	Private Sub ShowProgramHelp()

		Try

			Console.WriteLine("This program processes DMS analysis jobs for PRISM. Normal operation is to run the program without any command line switches.")
			Console.WriteLine()
			Console.WriteLine("Program syntax:" & ControlChars.NewLine & System.IO.Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().Location) & " [/T] [/Q]")
			Console.WriteLine()

			Console.WriteLine("Use /T to start the program in code test mode.")
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

End Module




