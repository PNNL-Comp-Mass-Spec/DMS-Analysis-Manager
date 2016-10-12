Option Strict On

Imports AnalysisManagerBase
Imports PHRPReader

Public Class clsPHRPMassErrorValidator

#Region "Module variables"

    Protected mErrorMessage As String = String.Empty
    Protected ReadOnly mDebugLevel As Integer

    ' This is a value between 0 and 100
    Protected Const mErrorThresholdPercent As Double = 5

    Protected WithEvents mPHRPReader As clsPHRPReader

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

    Public Sub New(intDebugLevel As Integer)
        mDebugLevel = intDebugLevel
    End Sub

    Protected Sub InformLargeErrorExample(massErrorEntry As KeyValuePair(Of Double, String))
        ShowErrorMessage("  ... large error example: " & massErrorEntry.Key & " Da for " & massErrorEntry.Value)
    End Sub

    Protected Function LoadSearchEngineParameters(
       objPHRPReader As clsPHRPReader,
       strSearchEngineParamFilePath As String,
       eResultType As clsPHRPReader.ePeptideHitResultType) As clsSearchEngineParameters

        Dim objSearchEngineParams As clsSearchEngineParameters = Nothing
        Dim blnSuccess As Boolean

        Try

            If String.IsNullOrEmpty(strSearchEngineParamFilePath) Then
                ShowWarningMessage("Search engine parameter file not defined; will assume a maximum tolerance of 10 Da")
                objSearchEngineParams = New clsSearchEngineParameters(eResultType.ToString())
                objSearchEngineParams.AddUpdateParameter("peptide_mass_tol", "10")
            Else

                blnSuccess = objPHRPReader.PHRPParser.LoadSearchEngineParameters(strSearchEngineParamFilePath, objSearchEngineParams)

                If Not blnSuccess Then
                    ShowWarningMessage("Error loading search engine parameter file " & IO.Path.GetFileName(strSearchEngineParamFilePath) & "; will assume a maximum tolerance of 10 Da")
                    objSearchEngineParams = New clsSearchEngineParameters(eResultType.ToString())
                    objSearchEngineParams.AddUpdateParameter("peptide_mass_tol", "10")
                End If
            End If

        Catch ex As Exception
            ShowErrorMessage("Error in LoadSearchEngineParameters", ex)
        End Try

        Return objSearchEngineParams

    End Function

    Protected Sub ShowErrorMessage(strMessage As String)
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMessage)
    End Sub

    Protected Sub ShowErrorMessage(strMessage As String, ex As Exception)
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
    ''' <param name="strSearchEngineParamFilePath"></param>
    ''' <returns>True if less than mErrorThresholdPercent of the data is bad; False otherwise</returns>
    ''' <remarks></remarks>
    Public Function ValidatePHRPResultMassErrors(strInputFilePath As String, eResultType As clsPHRPReader.ePeptideHitResultType, strSearchEngineParamFilePath As String) As Boolean

        Try
            mErrorMessage = String.Empty

            Dim oPeptideMassCalculator = New clsPeptideMassCalculator()

            Dim oStartupOptions = New clsPHRPStartupOptions() With {
                .LoadModsAndSeqInfo = True,
                .LoadMSGFResults = False,
                .LoadScanStatsData = False,
                .MaxProteinsPerPSM = 1,
                .PeptideMassCalculator = oPeptideMassCalculator
            }

            mPHRPReader = New clsPHRPReader(strInputFilePath, eResultType, oStartupOptions)

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
            mPHRPReader.SkipDuplicatePSMs = True

            ' Load the search engine parameters
            Dim objSearchEngineParams = LoadSearchEngineParameters(mPHRPReader, strSearchEngineParamFilePath, eResultType)

            ' Check for a custom charge carrier mass
            Dim customChargeCarrierMass As Double
            If clsPHRPParserMSGFDB.GetCustomChargeCarrierMass(objSearchEngineParams, customChargeCarrierMass) Then
                If mDebugLevel >= 2 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                                     String.Format("Custom charge carrier mass defined: {0:F3} Da", customChargeCarrierMass))
                End If
                oPeptideMassCalculator.ChargeCarrierMass = customChargeCarrierMass
            End If

            ' Define the precursor mass tolerance threshold
            ' At a minimum, use 6 Da, though we'll bump that up by 1 Da for each charge state (7 Da for CS 2, 8 Da for CS 3, 9 Da for CS 4, etc.)
            ' However, for MSGF+ we require that the masses match within 0.1 Da because the IsotopeError column allows for a more accurate comparison
            Dim dblPrecursorMassTolerance As Double = objSearchEngineParams.PrecursorMassToleranceDa

            If dblPrecursorMassTolerance < 6 Then
                dblPrecursorMassTolerance = 6
            End If

            If mDebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                                     "Will use mass tolerance of " & dblPrecursorMassTolerance.ToString("0.0") & " Da when determining PHRP mass errors")
            End If

            ' Count the number of PSMs with a mass error greater than dblPrecursorMassTolerance

            Dim intErrorCount = 0
            Dim intPsmCount = 0
            Dim dtLastProgress As DateTime = Date.UtcNow

            Dim strPeptideDescription As String
            Dim lstLargestMassErrors = New SortedDictionary(Of Double, String)

            While mPHRPReader.MoveNext

                '' This is old code that was in LoadSearchEngineParameters and was called after all PSMs had been cached in memory
                '' Since we're no longer pre-caching PSMs in memory, this code block was moved to this function
                '' However, I don't think this code is really needed, so I've commented it out
                ''
                '' Make sure mSearchEngineParams.ModInfo is up-to-date
                'If mPHRPReader.CurrentPSM.ModifiedResidues.Count > 0 Then
                '	For Each objResidue As PHRPReader.clsAminoAcidModInfo In mPHRPReader.CurrentPSM.ModifiedResidues

                '		' Check whether .ModDefinition is present in objSearchEngineParams.ModInfo
                '		Dim blnMatchFound = False
                '		For Each objKnownMod As PHRPReader.clsModificationDefinition In objSearchEngineParams.ModInfo
                '			If objKnownMod Is objResidue.ModDefinition Then
                '				blnMatchFound = True
                '				Exit For
                '			End If
                '		Next

                '		If Not blnMatchFound Then
                '			objSearchEngineParams.ModInfo.Add(objResidue.ModDefinition)
                '		End If
                '	Next

                'End If

                intPsmCount += 1

                If intPsmCount Mod 100 = 0 AndAlso Date.UtcNow.Subtract(dtLastProgress).TotalSeconds >= 15 Then
                    dtLastProgress = Date.UtcNow
                    Dim statusMessage = "Validating mass errors: " & mPHRPReader.PercentComplete.ToString("0.0") & "% complete"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, statusMessage)
                    Console.WriteLine(statusMessage)
                End If

                Dim objCurrentPSM As clsPSM = mPHRPReader.CurrentPSM

                If objCurrentPSM.PeptideMonoisotopicMass <= 0 Then
                    Continue While
                End If

                ' PrecursorNeutralMass is based on the mass value reported by the search engine 
                '   (will be reported mono mass or could be m/z or MH converted to neutral mass)
                ' PeptideMonoisotopicMass is the mass value computed by PHRP based on .PrecursorNeutralMass plus any modification masses associated with residues
                Dim dblMassError = objCurrentPSM.PrecursorNeutralMass - objCurrentPSM.PeptideMonoisotopicMass
                Dim dblToleranceCurrent As Double

                Dim psmIsotopeError As String = Nothing
                If eResultType = clsPHRPReader.ePeptideHitResultType.MSGFDB AndAlso objCurrentPSM.TryGetScore("IsotopeError", psmIsotopeError) Then
                    ' The integer value of dblMassError should match psmIsotopeError
                    ' However, scale up the tolerance based on the peptide mass
                    dblToleranceCurrent = 0.1 + objCurrentPSM.PeptideMonoisotopicMass / 50000.0
                    dblMassError -= CInt(psmIsotopeError)
                Else
                    dblToleranceCurrent = dblPrecursorMassTolerance + objCurrentPSM.Charge - 1
                End If

                If Math.Abs(dblMassError) <= dblToleranceCurrent Then
                    Continue While
                End If

                strPeptideDescription = "Scan=" & objCurrentPSM.ScanNumberStart & ", charge=" & objCurrentPSM.Charge & ", peptide=" & objCurrentPSM.PeptideWithNumericMods
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

            End While

            mPHRPReader.Dispose()

            If intPsmCount = 0 Then
                ShowWarningMessage("PHRPReader did not find any records in " & IO.Path.GetFileName(strInputFilePath))
                Return True
            End If

            Dim dblPercentInvalid = intErrorCount / intPsmCount * 100

            If intErrorCount <= 0 Then
                If mDebugLevel >= 2 Then
                    ShowMessage(
                        "All " & intPsmCount & " peptides have a mass error below " &
                        dblPrecursorMassTolerance.ToString("0.0") & " Da")
                End If
                Return True
            End If

            mErrorMessage = dblPercentInvalid.ToString("0.0") & "% of the peptides have a mass error over " &
                            dblPrecursorMassTolerance.ToString("0.0") & " Da"

            Dim warningMessage = mErrorMessage & " (" & intErrorCount & " / " & intPsmCount & ")"

            If dblPercentInvalid <= mErrorThresholdPercent Then
                ShowWarningMessage(warningMessage & "; this value is within tolerance")

                ' Blank out mErrorMessage since only a warning
                mErrorMessage = String.Empty
                Return True
            End If

            ShowErrorMessage(warningMessage & "; this value is too large (over " & mErrorThresholdPercent.ToString("0.0") & "%)")

            ' Log the first, last, and middle entry in lstLargestMassErrors
            InformLargeErrorExample(lstLargestMassErrors.First)

            If lstLargestMassErrors.Count > 1 Then
                InformLargeErrorExample(lstLargestMassErrors.Last)

                If lstLargestMassErrors.Count > 2 Then
                    Dim iterator = 0
                    For Each massError In lstLargestMassErrors
                        iterator += 1
                        If iterator >= lstLargestMassErrors.Count / 2 Then
                            InformLargeErrorExample(massError)
                            Exit For
                        End If
                    Next
                End If

            End If

            Return False

        Catch ex As Exception
            ShowErrorMessage("Error in ValidatePHRPResultMassErrors", ex)
            mErrorMessage = "Exception in ValidatePHRPResultMassErrors"
            Return False
        End Try

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
