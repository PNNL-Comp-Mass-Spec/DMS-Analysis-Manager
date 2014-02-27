Option Strict On

Imports AnalysisManagerBase

Public Class clsPHRPMassErrorValidator

#Region "Module variables"

	Protected mErrorMessage As String = String.Empty
	Protected mDatasetName As String
	Protected mWorkingDirectory As String
	Protected mDebugLevel As Integer

	' This is a value between 0 and 100
	Protected mErrorThresholdPercent As Double = 5

	Protected mPSMs As List(Of PHRPReader.clsPSM)

	Protected WithEvents mPHRPReader As PHRPReader.clsPHRPReader

#End Region

	Public ReadOnly Property ErrorMessage() As String
		Get
			Return mErrorMessage
		End Get
	End Property

	''' <summary>
	''' Value between 0 and 100
	''' If more than this percent of the data has a mass error larger than the threshold, then ValidatePHRPResultMassErrors returns false
	''' </summary>
	''' <value></value>
	''' <returns></returns>
	''' <remarks></remarks>
	Public ReadOnly Property ErrorThresholdPercent As Double
		Get
			Return mErrorThresholdPercent
		End Get
	End Property

	Public Sub New(ByVal strDatasetName As String, ByVal strWorkingDirectoryPath As String, ByVal intDebugLevel As Integer)
		mDatasetName = strDatasetName
		mWorkingDirectory = strWorkingDirectoryPath
		mDebugLevel = intDebugLevel
	End Sub

	Protected Sub InformLargeErrorExample(ByVal massErrorEntry As KeyValuePair(Of Double, String))
		ShowErrorMessage("  ... large error example: " & massErrorEntry.Key & " Da for " & massErrorEntry.Value)
	End Sub

	Protected Function LoadSearchEngineParameters(ByRef objPHRPReader As PHRPReader.clsPHRPReader, ByVal strSearchEngineParamFileName As String, ByVal eResultType As PHRPReader.clsPHRPReader.ePeptideHitResultType) As PHRPReader.clsSearchEngineParameters
		Dim objSearchEngineParams As PHRPReader.clsSearchEngineParameters = Nothing
		Dim blnSuccess As Boolean

		Try

			If String.IsNullOrEmpty(strSearchEngineParamFileName) Then
				ShowWarningMessage("Search engine parameter file not defined; will assume a maximum tolerance of 10 Da")
				objSearchEngineParams = New PHRPReader.clsSearchEngineParameters(eResultType.ToString())
				objSearchEngineParams.AddUpdateParameter("peptide_mass_tol", "10")
			Else

				blnSuccess = objPHRPReader.PHRPParser.LoadSearchEngineParameters(strSearchEngineParamFileName, objSearchEngineParams)

				If Not blnSuccess Then
					ShowWarningMessage("Error loading search engine parameter file " & strSearchEngineParamFileName & "; will assume a maximum tolerance of 10 Da")
					objSearchEngineParams = New PHRPReader.clsSearchEngineParameters(eResultType.ToString())
					objSearchEngineParams.AddUpdateParameter("peptide_mass_tol", "10")
				End If
			End If

			' Make sure mSearchEngineParams.ModInfo is up-to-date

			Dim blnMatchFound As Boolean

			For Each oPSMEntry As PHRPReader.clsPSM In mPSMs

				If oPSMEntry.ModifiedResidues.Count > 0 Then
					For Each objResidue As PHRPReader.clsAminoAcidModInfo In oPSMEntry.ModifiedResidues

						' Check whether .ModDefinition is present in objSearchEngineParams.ModInfo
						blnMatchFound = False
						For Each objKnownMod As PHRPReader.clsModificationDefinition In objSearchEngineParams.ModInfo
							If objKnownMod Is objResidue.ModDefinition Then
								blnMatchFound = True
								Exit For
							End If
						Next

						If Not blnMatchFound Then
							objSearchEngineParams.ModInfo.Add(objResidue.ModDefinition)
						End If
					Next

				End If
			Next

		Catch ex As Exception
			ShowErrorMessage("Error in LoadSearchEngineParameters", ex)
		End Try

		Return objSearchEngineParams

	End Function

	Protected Sub ShowErrorMessage(ByVal strMessage As String)
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMessage)
	End Sub

	Protected Sub ShowErrorMessage(ByVal strMessage As String, ByVal ex As System.Exception)
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMessage, ex)
	End Sub

	Protected Sub ShowMessage(strMessage As String)
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strMessage)
	End Sub

	Protected Sub ShowWarningMessage(strWarningMessage As String)
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strWarningMessage)
	End Sub

	''' <summary>
	''' Parses strInputFilePath to count the number of entries where the difference in mass between the precursor neutral mass value and the computed monoisotopic mass value is more than 6 Da away (more for higher charge states)
	''' </summary>
	''' <param name="strInputFilePath"></param>
	''' <param name="eResultType"></param>
	''' <param name="strSearchEngineParamFileName"></param>
	''' <returns>True if less than mErrorThresholdPercent of the data is bad; False otherwise</returns>
	''' <remarks></remarks>
	Public Function ValidatePHRPResultMassErrors(ByVal strInputFilePath As String, ByVal eResultType As PHRPReader.clsPHRPReader.ePeptideHitResultType, ByVal strSearchEngineParamFileName As String) As Boolean

		Dim blnSuccess As Boolean
		Dim objSearchEngineParams As PHRPReader.clsSearchEngineParameters

		Try
			mErrorMessage = String.Empty
			mPSMs = New List(Of PHRPReader.clsPSM)

			mPHRPReader = New PHRPReader.clsPHRPReader(strInputFilePath, eResultType, blnLoadModsAndSeqInfo:=True, blnLoadMSGFResults:=False, blnLoadScanStats:=False)

			' Report any errors cached during instantiation of mPHRPReader
			For Each strMessage As String In mPHRPReader.ErrorMessages
				If String.IsNullOrEmpty(mErrorMessage) Then
					mErrorMessage = String.Copy(strMessage)
				End If
				ShowErrorMessage(strMessage)
			Next
			If mPHRPReader.ErrorMessages.Count > 0 Then Return False

			' Report any warnings cached during instantiation of mPHRPReader
			For Each strMessage As String In mPHRPReader.WarningMessages
				If strMessage.StartsWith("Warning, taxonomy file not found") Then
					' Ignore this warning; the taxonomy file would have been used to determine the fasta file that was searched
					' We don't need that information in this application
				Else
					ShowWarningMessage(strMessage)
				End If
			Next

			mPHRPReader.ClearErrors()
			mPHRPReader.ClearWarnings()

			While mPHRPReader.MoveNext

				Dim objCurrentPSM As PHRPReader.clsPSM = mPHRPReader.CurrentPSM

				mPSMs.Add(objCurrentPSM)

			End While

			mPHRPReader.Dispose()

			If mPSMs.Count = 0 Then
				ShowWarningMessage("PHRPReader did not find any records in " & System.IO.Path.GetFileName(strInputFilePath))
				Return True
			End If

			' Load the search engine parameters
			objSearchEngineParams = LoadSearchEngineParameters(mPHRPReader, strSearchEngineParamFileName, eResultType)

			' Define the precursor mass tolerance threshold
			' At a minimum, use 6 Da, though we'll bump that up by 1 Da for each charge state (7 Da for CS 2, 8 Da for CS 3, 9 Da for CS 4, etc.)
			Dim dblPrecursorMassTolerance As Double
			dblPrecursorMassTolerance = objSearchEngineParams.PrecursorMassToleranceDa
			If dblPrecursorMassTolerance < 6 Then
				dblPrecursorMassTolerance = 6
			End If

			If mDebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Will use mass tolerance of " & dblPrecursorMassTolerance.ToString("0.0") & " Da when determining PHRP mass errors")
			End If

			' Count the number of entries in mPSMs with a mass error greater than dblPrecursorMassTolerance
			Dim dblMassError As Double
			Dim dblToleranceCurrent As Double

			Dim intErrorCount As Integer = 0

			Dim strPeptideDescription As String
			Dim lstLargestMassErrors = New SortedDictionary(Of Double, String)

			For Each oPSMEntry As PHRPReader.clsPSM In mPSMs

				If oPSMEntry.PeptideMonoisotopicMass > 0 Then
					dblMassError = oPSMEntry.PrecursorNeutralMass - oPSMEntry.PeptideMonoisotopicMass

					dblToleranceCurrent = dblPrecursorMassTolerance + oPSMEntry.Charge - 1
					If Math.Abs(dblMassError) > dblToleranceCurrent Then

						strPeptideDescription = "Scan=" & oPSMEntry.ScanNumberStart & ", charge=" & oPSMEntry.Charge & ", peptide=" & oPSMEntry.PeptideWithNumericMods
						intErrorCount += 1

						' Keep track of the 100 largest mass errors
						If lstLargestMassErrors.Count < 100 Then
							If Not lstLargestMassErrors.ContainsKey(dblMassError) Then
								lstLargestMassErrors.Add(dblMassError, strPeptideDescription)
							End If
						Else

							Dim dblMinValue As Double = lstLargestMassErrors.Keys.Min()
							If dblMassError > dblMinValue AndAlso Not lstLargestMassErrors.ContainsKey(dblMassError) Then
								lstLargestMassErrors.Remove(dblMinValue)
								lstLargestMassErrors.Add(dblMassError, strPeptideDescription)
							End If

						End If

					End If

				End If
			Next


			Dim dblPercentInvalid As Double
			dblPercentInvalid = intErrorCount / mPSMs.Count * 100

			If intErrorCount > 0 Then

				Dim strMessage As String
				mErrorMessage = dblPercentInvalid.ToString("0.0") & "% of the peptides have a mass error over " & dblPrecursorMassTolerance.ToString("0.0") & " Da"
				strMessage = mErrorMessage & " (" & intErrorCount & " / " & mPSMs.Count & ")"

				If dblPercentInvalid > mErrorThresholdPercent Then
					ShowErrorMessage(strMessage & "; this value is too large (over " & mErrorThresholdPercent.ToString("0.0") & "%)")

					' Log the first, last, and middle entry in lstLargestMassErrors
					InformLargeErrorExample(lstLargestMassErrors.First)

					If lstLargestMassErrors.Count > 1 Then
						InformLargeErrorExample(lstLargestMassErrors.Last)

						If lstLargestMassErrors.Count > 2 Then
							Dim iterator As Integer = 0
							For Each massError In lstLargestMassErrors
								iterator += 1
								If iterator >= lstLargestMassErrors.Count / 2 Then
									InformLargeErrorExample(massError)
									Exit For
								End If
							Next
						End If

					End If


					blnSuccess = False
				Else
					ShowWarningMessage(strMessage & "; this value is within tolerance")

					' Blank out mErrorMessage since only a warning
					mErrorMessage = String.Empty
					blnSuccess = True
				End If

			Else
				If mDebugLevel >= 2 Then
					ShowMessage("All " & mPSMs.Count & " peptides have a mass error below " & dblPrecursorMassTolerance.ToString("0.0") & " Da")
				End If
				blnSuccess = True
			End If

		Catch ex As System.Exception
			ShowErrorMessage("Error in LoadSearchEngineParameters", ex)
			mErrorMessage = "Exception in LoadSearchEngineParameters"
			Return False
		End Try

		Return blnSuccess

	End Function

	Private Sub mPHRPReader_ErrorEvent(strErrorMessage As String) Handles mPHRPReader.ErrorEvent
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strErrorMessage)
	End Sub

	Private Sub mPHRPReader_MessageEvent(strMessage As String) Handles mPHRPReader.MessageEvent
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strMessage)
	End Sub

	Private Sub mPHRPReader_WarningEvent(strWarningMessage As String) Handles mPHRPReader.WarningEvent
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strWarningMessage)
	End Sub
End Class
