Option Strict On

Public Class clsCDTAUtilities

	Protected WithEvents m_CDTACondenser As CondenseCDTAFile.clsCDTAFileCondenser

	''' <summary>
	''' Removes any spectra with 2 or fewer ions in a _DTA.txt ifle
	''' </summary>
	''' <param name="strWorkDir">Folder with the CDTA file</param>
	''' <param name="strInputFileName">CDTA filename</param>
	''' <returns>True if success; false if an error</returns>
	Public Function RemoveSparseSpectra(ByVal strWorkDir As String, ByVal strInputFileName As String) As Boolean

		Const MINIMUM_ION_COUNT As Integer = 3

		Dim strSourceFilePath As String
		Dim fiOriginalFile As IO.FileInfo
		Dim fiUpdatedFile As IO.FileInfo

		Dim strLineIn As String

		Dim blnParentIonLineIsNext As Boolean

		Dim intIonCount As Integer = 0
		Dim intSpectraParsed As Integer = 0
		Dim intSpectraRemoved As Integer = 0

		Dim sbCurrentSpectrum As Text.StringBuilder = New Text.StringBuilder

		Try

			strSourceFilePath = IO.Path.Combine(strWorkDir, strInputFileName)

			If String.IsNullOrEmpty(strWorkDir) Then
				ReportError("Error in RemoveSparseSpectra: strWorkDir is empty")
				Return False
			End If

			If String.IsNullOrEmpty(strInputFileName) Then
				ReportError("Error in RemoveSparseSpectra: strInputFileName is empty")
				Return False
			End If

			fiOriginalFile = New IO.FileInfo(strSourceFilePath)
			If Not fiOriginalFile.Exists Then
				ReportError("Error in RemoveSparseSpectra: source file not found: " + strSourceFilePath)
				Return False
			End If

			fiUpdatedFile = New IO.FileInfo(strSourceFilePath + ".tmp")

			' Open the input file
			Using srInFile As IO.StreamReader = New IO.StreamReader(New IO.FileStream(fiOriginalFile.FullName, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

				' Create the output file
				Using swOutFile As IO.StreamWriter = New IO.StreamWriter(New IO.FileStream(fiUpdatedFile.FullName, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

					Do While srInFile.Peek > -1
						strLineIn = srInFile.ReadLine()

						If String.IsNullOrEmpty(strLineIn) Then
							sbCurrentSpectrum.AppendLine()
						Else
							If strLineIn.StartsWith("="c) Then

								' DTA header line, for example:
								' =================================== "H20120523_JQ_CPTAC2_4TP_Exp1_IMAC_01.0002.0002.3.dta" ==================================

								If sbCurrentSpectrum.Length > 0 Then
									If intIonCount >= MINIMUM_ION_COUNT OrElse intSpectraParsed = 0 Then
										' Write the cached spectrum
										swOutFile.Write(sbCurrentSpectrum.ToString)
									Else
										intSpectraRemoved += 1
									End If
									sbCurrentSpectrum.Clear()
									intIonCount = 0
								End If

								blnParentIonLineIsNext = True
								intSpectraParsed += 1

							ElseIf blnParentIonLineIsNext Then
								' strLineIn contains the parent ion line text

								blnParentIonLineIsNext = False
							Else
								' Line is not a header or the parent ion line
								' Assume a data line
								intIonCount += 1
							End If

							sbCurrentSpectrum.AppendLine(strLineIn)

						End If
					Loop

					If sbCurrentSpectrum.Length > 0 Then
						If intIonCount >= MINIMUM_ION_COUNT Then
							' Write the cached spectrum
							swOutFile.Write(sbCurrentSpectrum.ToString)
						Else
							intSpectraRemoved += 1
						End If
					End If

				End Using
			End Using

			Dim blnSpectraRemoved As Boolean = False
			Dim blnReplaceSourceFile As Boolean = True
			Dim blnDeleteSourceFileIfUpdated As Boolean = True

			If intSpectraRemoved > 0 Then
				ReportInfo("Removed " & intSpectraRemoved & " spectra from " & strInputFileName & " since fewer than " & MINIMUM_ION_COUNT & " ions", 1)
				blnSpectraRemoved = True
			End If

			FinalizeCDTAValidation(blnSpectraRemoved, blnReplaceSourceFile, blnDeleteSourceFileIfUpdated, fiOriginalFile, fiUpdatedFile)

		Catch ex As Exception
			ReportError("Exception in RemoveSparseSpectra: " + ex.Message)
			Return False
		End Try

		Return True

	End Function
	''' <summary>
	''' Replaces the original file with a new CDTA file if blnNewCDTAFileHasUpdates=True; deletes the new CDTA file if blnNewCDTAFileHasUpdates=false
	''' </summary>
	''' <param name="blnNewCDTAFileHasUpdates">True if the new CDTA file has updated info</param>
	''' <param name="blnReplaceSourceFile">If True, then replaces the source file with and updated file</param>
	''' <param name="blnDeleteSourceFileIfUpdated">Only valid if blnReplaceSourceFile=True: If True, then the source file is deleted if an updated version is created. If false, then the source file is renamed to .old if an updated version is created.</param>
	''' <param name="fiOriginalFile">File handle to the original CDTA file</param>
	''' <param name="fiUpdatedFile">File handle to the new CDTA file</param>
	''' <remarks></remarks>
	Protected Sub FinalizeCDTAValidation(ByVal blnNewCDTAFileHasUpdates As Boolean, ByVal blnReplaceSourceFile As Boolean, ByVal blnDeleteSourceFileIfUpdated As Boolean, ByVal fiOriginalFile As IO.FileInfo, ByVal fiUpdatedFile As IO.FileInfo)

		If blnNewCDTAFileHasUpdates Then
			System.Threading.Thread.Sleep(100)

			Dim strSourceFilePath As String = fiOriginalFile.FullName

			If blnReplaceSourceFile Then
				' Replace the original file with the new one
				Dim strOldFilePath As String
				Dim intAddon As Integer = 0

				Do
					strOldFilePath = fiOriginalFile.FullName + ".old"
					If intAddon > 0 Then
						strOldFilePath &= intAddon.ToString()
					End If
					intAddon += 1
				Loop While IO.File.Exists(strOldFilePath)

				fiOriginalFile.MoveTo(strOldFilePath)
				System.Threading.Thread.Sleep(100)

				fiUpdatedFile.MoveTo(strSourceFilePath)

				If blnDeleteSourceFileIfUpdated Then
					System.Threading.Thread.Sleep(125)
					PRISM.Processes.clsProgRunner.GarbageCollectNow()

					fiOriginalFile.Delete()
				End If


			Else
				' Directly wrote to the output file; nothing to rename

			End If
		Else
			' No changes were made; nothing to update
			' However, delete the new file we created
			System.Threading.Thread.Sleep(125)
			PRISM.Processes.clsProgRunner.GarbageCollectNow()

			fiUpdatedFile.Delete()

		End If

	End Sub

	''' <summary>
	''' Makes sure the specified _DTA.txt file has scan=x and cs=y tags in the parent ion line
	''' </summary>
	''' <param name="strSourceFilePath">Input _DTA.txt file to parse</param>
	''' <param name="blnReplaceSourceFile">If True, then replaces the source file with and updated file</param>
	''' <param name="blnDeleteSourceFileIfUpdated">Only valid if blnReplaceSourceFile=True: If True, then the source file is deleted if an updated version is created. If false, then the source file is renamed to .old if an updated version is created.</param>
	''' <param name="strOutputFilePath">Output file path to use for the updated file; required if blnReplaceSourceFile=False; ignored if blnReplaceSourceFile=True</param>
	''' <returns>True if success; false if an error</returns>
	Public Function ValidateCDTAFileScanAndCSTags(ByVal strSourceFilePath As String, ByVal blnReplaceSourceFile As Boolean, ByVal blnDeleteSourceFileIfUpdated As Boolean, ByRef strOutputFilePath As String) As Boolean

		Dim strOutputFilePathTemp As String
		Dim strLineIn As String
		Dim strDTAHeader As String

		Dim intScanNumberStart As Integer
		Dim intScanNumberEnd As Integer
		Dim intScanCount As Integer
		Dim intCharge As Integer

		Dim blnValidScanInfo As Boolean = False
		Dim blnParentIonLineIsNext As Boolean = False
		Dim blnParentIonLineUpdated As Boolean = False

		Dim blnSuccess As Boolean = False

		' We use the DtaTextFileReader to parse out the scan and charge from the header line
		Dim objReader As MSDataFileReader.clsDtaTextFileReader

		Dim fiOriginalFile As IO.FileInfo
		Dim fiUpdatedFile As IO.FileInfo

		Try

			If String.IsNullOrEmpty(strSourceFilePath) Then
				ReportError("Error in ValidateCDTAFileScanAndCSTags: strSourceFilePath is empty")
				Return False
			End If

			fiOriginalFile = New IO.FileInfo(strSourceFilePath)
			If Not fiOriginalFile.Exists Then
				ReportError("Error in ValidateCDTAFileScanAndCSTags: source file not found: " + strSourceFilePath)
				Return False
			End If

			If blnReplaceSourceFile Then
				strOutputFilePathTemp = strSourceFilePath + ".tmp"
			Else
				' strOutputFilePath must contain a valid file path
				If String.IsNullOrEmpty(strOutputFilePath) Then
					ReportError("Error in ValidateCDTAFileScanAndCSTags: variable strOutputFilePath must define a file path when blnReplaceSourceFile=False")
					Return False
				End If
				strOutputFilePathTemp = strOutputFilePath
			End If

			fiUpdatedFile = New IO.FileInfo(strOutputFilePathTemp)

			objReader = New MSDataFileReader.clsDtaTextFileReader(False)

			' Open the input file
			Using srInFile As IO.StreamReader = New IO.StreamReader(New IO.FileStream(fiOriginalFile.FullName, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

				' Create the output file
				Using swOutFile As IO.StreamWriter = New IO.StreamWriter(New IO.FileStream(fiUpdatedFile.FullName, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

					Do While srInFile.Peek > -1
						strLineIn = srInFile.ReadLine()

						If String.IsNullOrEmpty(strLineIn) Then
							swOutFile.WriteLine()
						Else
							If strLineIn.StartsWith("="c) Then
								' Parse the DTA header line, for example:
								' =================================== "H20120523_JQ_CPTAC2_4TP_Exp1_IMAC_01.0002.0002.3.dta" ==================================

								' Remove the leading and trailing characters, then extract the scan and charge
								strDTAHeader = strLineIn.Trim(New Char() {"="c, " "c, ControlChars.Quote})
								blnValidScanInfo = objReader.ExtractScanInfoFromDtaHeader(strDTAHeader, intScanNumberStart, intScanNumberEnd, intScanCount, intCharge)

								blnParentIonLineIsNext = True

							ElseIf blnParentIonLineIsNext Then
								' strLineIn contains the parent ion line text

								' Construct the parent ion line to write out
								' Will contain the MH+ value of the parent ion (thus always the 1+ mass, even if actually a different charge)
								' Next contains the charge state, then scan= and cs= tags, for example:
								' 447.34573 1   scan=3 cs=1

								If Not strLineIn.Contains("scan=") Then
									' Append scan=x to the parent ion line
									strLineIn = strLineIn.Trim() + "   scan=" + intScanNumberStart.ToString()
									blnParentIonLineUpdated = True
								End If

								If Not strLineIn.Contains("cs=") Then
									' Append cs=y to the parent ion line
									strLineIn = strLineIn.Trim() + " cs=" + intCharge.ToString()
									blnParentIonLineUpdated = True
								End If

								blnParentIonLineIsNext = False

							End If

							swOutFile.WriteLine(strLineIn)

						End If
					Loop

				End Using
			End Using

			FinalizeCDTAValidation(blnParentIonLineUpdated, blnReplaceSourceFile, blnDeleteSourceFileIfUpdated, fiOriginalFile, fiUpdatedFile)

			blnSuccess = True

		Catch ex As Exception
			ReportError("Exception in ValidateCDTAFileScanAndCSTags: " + ex.Message)
			Return False
		End Try

		Return blnSuccess

	End Function

	''' <summary>
	''' Condenses CDTA files that are over 2 GB in size
	''' </summary>
	''' <param name="strWorkDir">Folder with the CDTA file</param>
	''' <param name="strInputFileName">CDTA filename</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Public Function ValidateCDTAFileSize(ByVal strWorkDir As String, ByVal strInputFileName As String) As Boolean
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
				ReportError("_DTA.txt file not found: " & strInputFilePath)
				Return False
			End If

			If ioFileInfo.Length >= FILE_SIZE_THRESHOLD Then
				' Need to condense the file

				strMessage = ioFileInfo.Name & " is " & CSng(ioFileInfo.Length / 1024 / 1024 / 1024).ToString("0.00") & " GB in size; will now condense it by combining data points with consecutive zero-intensity values"
				ReportInfo(strMessage, 0)

				m_CDTACondenser = New CondenseCDTAFile.clsCDTAFileCondenser

				blnSuccess = m_CDTACondenser.ProcessFile(ioFileInfo.FullName, ioFileInfo.DirectoryName)

				If Not blnSuccess Then
					ReportError("Error condensing _DTA.txt file: " & m_CDTACondenser.GetErrorMessage())
					Return False
				Else
					' Wait 500 msec, then check the size of the new _dta.txt file
					System.Threading.Thread.Sleep(500)

					ioFileInfo.Refresh()

					ReportInfo("Condensing complete; size of the new _dta.txt file is " & CSng(ioFileInfo.Length / 1024 / 1024 / 1024).ToString("0.00") & " GB", 1)

					Try
						strFilePathOld = System.IO.Path.Combine(strWorkDir, System.IO.Path.GetFileNameWithoutExtension(ioFileInfo.FullName) & "_Old.txt")

						ReportInfo("Now deleting file " & strFilePathOld, 2)

						ioFileInfo = New System.IO.FileInfo(strFilePathOld)
						If ioFileInfo.Exists Then
							ioFileInfo.Delete()
						Else
							ReportError("Old _DTA.txt file not found:" & ioFileInfo.FullName & "; cannot delete")
						End If

					Catch ex As Exception
						' Error deleting the file; log it but keep processing
						ReportWarning("Exception deleting _dta_old.txt file: " & ex.Message)
					End Try

				End If
			End If

			blnSuccess = True

		Catch ex As Exception
			ReportError("Exception in ValidateCDTAFileSize: " & ex.Message)
			Return False
		End Try

		Return blnSuccess

	End Function

#Region "Event Handlers"

	Private Sub m_CDTACondenser_ProgressChanged(ByVal taskDescription As String, ByVal percentComplete As Single) Handles m_CDTACondenser.ProgressChanged

		ReportProgress(taskDescription, percentComplete)
	
	End Sub

#End Region

#Region "Events"
	Public Event ErrorEvent(ByVal ErrorMessage As String)
	Public Event InfoEvent(ByVal Message As String, DebugLevel As Integer)
	Public Event ProgressEvent(ByVal taskDescription As String, ByVal PercentComplete As Single)
	Public Event WarningEvent(ByVal Message As String)

	Protected Sub ReportError(ByVal strErrorMessage As String)
		RaiseEvent ErrorEvent(strErrorMessage)
	End Sub

	Protected Sub ReportInfo(ByVal strMessage As String, ByVal intDebugLevel As Integer)
		RaiseEvent InfoEvent(strMessage, intDebugLevel)
	End Sub

	Protected Sub ReportProgress(ByVal taskDescription As String, ByVal PercentComplete As Single)
		RaiseEvent ProgressEvent(taskDescription, PercentComplete)
	End Sub

	Protected Sub ReportWarning(ByVal strMessage As String)
		RaiseEvent WarningEvent(strMessage)
	End Sub
#End Region

End Class
