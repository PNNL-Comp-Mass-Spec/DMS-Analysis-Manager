Option Strict On

' This class can be used to compute an estimate of the baseline noise level in a series of intensity values
' Use ComputeNoiseLevel for mass spectra data (intensity values only)
' Use ComputeMultiSegmentNoiseLevel for mass spectra data when you want separate S/N values returned for different parts of the mass spectrum
' Use ComputeDualTrimmedNoiseLevelTTest for time-series data
'
' -------------------------------------------------------------------------------
' Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
' Copyright 2008, Battelle Memorial Institute.  All Rights Reserved.

' E-mail: matthew.monroe@pnl.gov or matt@alchemistmatt.com
' Website: http://ncrr.pnl.gov/ or http://www.sysbio.org/resources/staff/
' -------------------------------------------------------------------------------
' 
' Licensed under the Apache License, Version 2.0; you may not use this file except
' in compliance with the License.  You may obtain a copy of the License at 
' http://www.apache.org/licenses/LICENSE-2.0

Public Class clsBaselineNoiseEstimator
#Region "Constants and Enums"
    Public Enum eNoiseThresholdModes
        AbsoluteThreshold = 0
        TrimmedMeanByAbundance = 1
        TrimmedMeanByCount = 2
        TrimmedMedianByAbundance = 3
        DualTrimmedMeanByAbundance = 4
        SegmentedTrimmedMedianByAbundance = 5
    End Enum

    Public Enum eTTestConfidenceLevelConstants
        Conf80Pct = 0
        Conf90Pct = 1
        Conf95Pct = 2
        Conf98Pct = 3
        Conf99Pct = 4
        Conf99_5Pct = 5
        Conf99_8Pct = 6
        Conf99_9Pct = 7
    End Enum
#End Region

#Region "Structures"
    Public Structure udtBaselineNoiseOptionsType
        Public BaselineNoiseMode As eNoiseThresholdModes                    ' Method to use to determine the baseline noise level
        Public BaselineNoiseLevelAbsolute As Single                         ' Explicitly defined noise intensity; only used if .BaselineNoiseMode = eNoiseThresholdModes.AbsoluteThreshold; 50000 for SIC, 0 for MS/MS spectra
        Public MinimumBaselineNoiseLevel As Single                          ' If the noise threshold computed is less than this value, then will use this value to compute S/N; additionally, this is used as the minimum intensity threshold when computing a trimmed noise level
        Public TrimmedMeanFractionLowIntensityDataToExamine As Single       ' Typically 0.75 for SICs, 0.5 for MS/MS spectra; only used for eNoiseThresholdModes.TrimmedMeanByAbundance, .TrimmedMeanByCount, .TrimmedMedianByAbundance
        Public DualTrimmedMeanStdDevLimits As Short                         ' Typically 5; distance from the mean in standard deviation units (SqrRt(Variance)) to discard data for computing the trimmed mean; only used for eNoiseThresholdModes.DualTrimmedMeanByAbundance
        Public DualTrimmedMeanMaximumSegments As Short                      ' Typically 3; set to 1 to disable segmentation; only used for eNoiseThresholdModes.DualTrimmedMeanByAbundance
        Public SegmentedTrimmedMedianMinimumPointsPerSegment As Integer     ' Typically 50; only used if .BaselineNoiseMode = eNoiseThresholdModes.SegmentedTrimmedMedianByAbundance
        Public SegmentedTrimmedMedianTargetSegmentWidthX As Single          ' Target X-range in each segment; 100 m/z is reasonable for MS data
        Public Sub InitializeToDefaults()
            BaselineNoiseMode = eNoiseThresholdModes.TrimmedMedianByAbundance
            BaselineNoiseLevelAbsolute = 0
            MinimumBaselineNoiseLevel = 1
            TrimmedMeanFractionLowIntensityDataToExamine = 0.75
            DualTrimmedMeanStdDevLimits = 5
            DualTrimmedMeanMaximumSegments = 3
            SegmentedTrimmedMedianMinimumPointsPerSegment = 100
            SegmentedTrimmedMedianTargetSegmentWidthX = 300
        End Sub
    End Structure

    Public Structure udtBaselineNoiseStatsType
        Public NoiseLevel As Single             ' Typically the average of the data being sampled to determine the baseline noise estimate
        Public NoiseStDev As Single             ' Standard Deviation of the data used to compute the baseline estimate
        Public PointsUsed As Integer
        Public NoiseThresholdModeUsed As eNoiseThresholdModes
    End Structure

    Public Structure udtBaselineNoiseStatSegmentsType
        Public BaselineNoiseStats As udtBaselineNoiseStatsType
        Public SegmentIndexStart As Integer
        Public SegmentIndexEnd As Integer
        Public SegmentMidpointValue As Single
        Public Sub ComputeMidPoint()
            If SegmentIndexStart = SegmentIndexEnd Then
                SegmentMidpointValue = SegmentIndexStart
            Else
                SegmentMidpointValue = SegmentIndexStart + CSng(SegmentIndexEnd - SegmentIndexStart) / 2
            End If
        End Sub
    End Structure
#End Region

    Public Function ComputeDualTrimmedNoiseLevelTTest(ByRef sngData() As Single, ByVal intIndexStart As Integer, ByVal intIndexEnd As Integer, ByVal udtBaselineNoiseOptions As udtBaselineNoiseOptionsType, ByRef udtBaselineNoiseStats() As udtBaselineNoiseStatSegmentsType) As Boolean

        ' Divide the data into the number of segments given by udtBaselineNoiseOptions.DualTrimmedMeanMaximumSegments  (use 3 by default)
        ' Call ComputeDualTrimmedNoiseLevel for each segment
        ' Use a TTest to determine whether we need to define a custom noise threshold for each segment
        '
        ' This noise estimator is best used for time-series data, like TICs, SICs, or BPIs

        Dim udtPrevSegmentStats As udtBaselineNoiseStatsType
        Dim eConfidenceLevel As eTTestConfidenceLevelConstants

        Dim intSegmentCountLocal As Integer
        Dim intSegmentLength As Integer
        Dim intSegmentIndex As Integer
        Dim intSegmentIndexCopy As Integer

        Dim dblTCalculated As Double
        Dim blnSignificantDifference As Boolean

        Try

            intSegmentCountLocal = udtBaselineNoiseOptions.DualTrimmedMeanMaximumSegments
            If intSegmentCountLocal = 0 Then intSegmentCountLocal = 3
            If intSegmentCountLocal < 1 Then intSegmentCountLocal = 1

            ReDim udtBaselineNoiseStats(intSegmentCountLocal - 1)

            ' Initialize BaselineNoiseStats for each segment now, in case an error occurs
            For intSegmentIndex = 0 To intSegmentCountLocal - 1
                InitializeBaselineNoiseStats(udtBaselineNoiseStats(intSegmentIndex).BaselineNoiseStats, udtBaselineNoiseOptions.MinimumBaselineNoiseLevel, eNoiseThresholdModes.DualTrimmedMeanByAbundance)
            Next intSegmentIndex

            ' Determine the segment length
            intSegmentLength = CInt(Math.Round((intIndexEnd - intIndexStart) / intSegmentCountLocal, 0))

            ' Initialize the first segment
            With udtBaselineNoiseStats(0)
                .SegmentIndexStart = intIndexStart
                If intSegmentCountLocal = 1 Then
                    .SegmentIndexEnd = intIndexEnd
                Else
                    .SegmentIndexEnd = .SegmentIndexStart + intSegmentLength - 1
                End If
                .ComputeMidPoint()
            End With

            ' Initialize the remaining segments
            For intSegmentIndex = 1 To intSegmentCountLocal - 1
                With udtBaselineNoiseStats(intSegmentIndex)
                    .SegmentIndexStart = udtBaselineNoiseStats(intSegmentIndex - 1).SegmentIndexEnd + 1
                    If intSegmentIndex = intSegmentCountLocal - 1 Then
                        .SegmentIndexEnd = intIndexEnd
                    Else
                        .SegmentIndexEnd = .SegmentIndexStart + intSegmentLength - 1
                    End If
                    .ComputeMidPoint()
                End With
            Next intSegmentIndex

            ' Call ComputeDualTrimmedNoiseLevel for each segment
            For intSegmentIndex = 0 To intSegmentCountLocal - 1
                With udtBaselineNoiseStats(intSegmentIndex)
                    ComputeDualTrimmedNoiseLevel(sngData, .SegmentIndexStart, .SegmentIndexEnd, udtBaselineNoiseOptions, .BaselineNoiseStats)
                End With
            Next intSegmentIndex

            ' Compare adjacent segments using a T-Test, starting with the final segment and working backward
            eConfidenceLevel = eTTestConfidenceLevelConstants.Conf90Pct
            intSegmentIndex = intSegmentCountLocal - 1
            Do While intSegmentIndex > 0
                udtPrevSegmentStats = udtBaselineNoiseStats(intSegmentIndex - 1).BaselineNoiseStats
                With udtBaselineNoiseStats(intSegmentIndex).BaselineNoiseStats
                    blnSignificantDifference = TestSignificanceUsingTTest(.NoiseLevel, udtPrevSegmentStats.NoiseLevel, .NoiseStDev, udtPrevSegmentStats.NoiseStDev, .PointsUsed, udtPrevSegmentStats.PointsUsed, eConfidenceLevel, dblTCalculated)
                End With

                If blnSignificantDifference Then
                    ' Significant difference; leave the 2 segments intact
                Else
                    ' Not a significant difference; recompute the Baseline Noise stats using the two segments combined
                    With udtBaselineNoiseStats(intSegmentIndex - 1)
                        .SegmentIndexEnd = udtBaselineNoiseStats(intSegmentIndex).SegmentIndexEnd
                        ComputeDualTrimmedNoiseLevel(sngData, .SegmentIndexStart, .SegmentIndexEnd, udtBaselineNoiseOptions, .BaselineNoiseStats)
                    End With

                    For intSegmentIndexCopy = intSegmentIndex To intSegmentCountLocal - 2
                        udtBaselineNoiseStats(intSegmentIndexCopy) = udtBaselineNoiseStats(intSegmentIndexCopy + 1)
                    Next intSegmentIndexCopy
                    intSegmentCountLocal -= 1
                End If
                intSegmentIndex -= 1
            Loop

            If intSegmentCountLocal <> udtBaselineNoiseStats.Length Then
                ReDim Preserve udtBaselineNoiseStats(intSegmentCountLocal - 1)
            End If
        Catch ex As Exception
            Return False
        End Try

        Return True

    End Function

    Protected Function ComputeDualTrimmedNoiseLevel(ByRef sngData() As Single, ByVal intIndexStart As Integer, ByVal intIndexEnd As Integer, ByVal udtBaselineNoiseOptions As udtBaselineNoiseOptionsType, ByRef udtBaselineNoiseStats As udtBaselineNoiseStatsType) As Boolean
        ' Computes the average of all of the data in sngData()
        ' Next, discards the data above and below udtBaselineNoiseOptions.DualTrimmedMeanStdDevLimits of the mean
        ' Finally, recomputes the average using the data that remains
        ' Returns True if success, False if error (or no data in sngData)

        ' Note: Replaces values of 0 with the minimum positive value in sngData()
        ' Note: You cannot use sngData.Length to determine the length of the array; use intIndexStart and intIndexEnd to find the limits

        Dim dblIntensityThresholdMin As Double
        Dim dblIntensityThresholdMax As Double

        Dim dblSum As Double
        Dim dblAverage As Double
        Dim dblVariance As Double

        Dim intIndex As Integer

        Dim intDataSortedCount As Integer
        Dim sngDataSorted() As Single
        Dim sngMinimumPositiveValue As Single

        Dim intDataSortedIndexStart As Integer
        Dim intDataSortedIndexEnd As Integer
        Dim intDataUsedCount As Integer

        ' Initialize udtBaselineNoiseStats
        InitializeBaselineNoiseStats(udtBaselineNoiseStats, udtBaselineNoiseOptions.MinimumBaselineNoiseLevel, eNoiseThresholdModes.DualTrimmedMeanByAbundance)

        If sngData Is Nothing OrElse intIndexEnd - intIndexStart < 0 Then
            Return False
        End If

        ' Copy the data into sngDataSorted
        intDataSortedCount = intIndexEnd - intIndexStart + 1
        ReDim sngDataSorted(intDataSortedCount - 1)

        For intIndex = intIndexStart To intIndexEnd
            sngDataSorted(intIndex - intIndexStart) = sngData(intIndex)
        Next intIndex

        ' Sort the array
        Array.Sort(sngDataSorted)

        ' Look for the minimum positive value and replace all data in sngDataSorted with that value
        sngMinimumPositiveValue = ReplaceSortedDataWithMinimumPositiveValue(intDataSortedCount, sngDataSorted)

        ' Initialize the indices to use in sngDataSorted()
        intDataSortedIndexStart = 0
        intDataSortedIndexEnd = intDataSortedCount - 1

        ' Compute the average using the data in sngDataSorted between intDataSortedIndexStart and intDataSortedIndexEnd (i.e. all the data)
        dblSum = 0
        For intIndex = intDataSortedIndexStart To intDataSortedIndexEnd
            dblSum += sngDataSorted(intIndex)
        Next intIndex
        intDataUsedCount = intDataSortedIndexEnd - intDataSortedIndexStart + 1
        dblAverage = dblSum / intDataUsedCount

        If intDataUsedCount > 1 Then
            ' Compute the variance (this is a sample variance, not a population variance)
            dblSum = 0
            For intIndex = intDataSortedIndexStart To intDataSortedIndexEnd
                dblSum += (sngDataSorted(intIndex) - dblAverage) ^ 2
            Next intIndex
            dblVariance = dblSum / (intDataUsedCount - 1)
        Else
            dblVariance = 0
        End If

        If udtBaselineNoiseOptions.DualTrimmedMeanStdDevLimits < 1 Then
            udtBaselineNoiseOptions.DualTrimmedMeanStdDevLimits = 1
        End If

        ' Note: Standard Deviation = sigma = SquareRoot(Variance)
        dblIntensityThresholdMin = dblAverage - Math.Sqrt(dblVariance) * udtBaselineNoiseOptions.DualTrimmedMeanStdDevLimits
        dblIntensityThresholdMax = dblAverage + Math.Sqrt(dblVariance) * udtBaselineNoiseOptions.DualTrimmedMeanStdDevLimits

        ' Recompute the average using only the data between dblIntensityThresholdMin and dblIntensityThresholdMax in sngDataSorted
        dblSum = 0
        intIndex = intDataSortedIndexStart
        Do While intIndex <= intDataSortedIndexEnd
            If sngDataSorted(intIndex) >= dblIntensityThresholdMin Then
                intDataSortedIndexStart = intIndex
                Do While intIndex <= intDataSortedIndexEnd
                    If sngDataSorted(intIndex) <= dblIntensityThresholdMax Then
                        dblSum += sngDataSorted(intIndex)
                    Else
                        intDataSortedIndexEnd = intIndex - 1
                        Exit Do
                    End If
                    intIndex += 1
                Loop
            End If
            intIndex += 1
        Loop
        intDataUsedCount = intDataSortedIndexEnd - intDataSortedIndexStart + 1

        If intDataUsedCount > 0 Then
            udtBaselineNoiseStats.NoiseLevel = CSng(dblSum / intDataUsedCount)

            ' Compute the variance (this is a sample variance, not a population variance)
            dblSum = 0
            For intIndex = intDataSortedIndexStart To intDataSortedIndexEnd
                dblSum += (sngDataSorted(intIndex) - udtBaselineNoiseStats.NoiseLevel) ^ 2
            Next intIndex

            With udtBaselineNoiseStats
                If intDataUsedCount > 1 Then
                    .NoiseStDev = CSng(Math.Sqrt(dblSum / (intDataUsedCount - 1)))
                Else
                    .NoiseStDev = 0
                End If
                .PointsUsed = intDataUsedCount
            End With

        Else
            udtBaselineNoiseStats.NoiseLevel = Math.Max(sngMinimumPositiveValue, udtBaselineNoiseOptions.MinimumBaselineNoiseLevel)
            udtBaselineNoiseStats.NoiseStDev = 0
        End If

        ' Assure that .NoiseLevel is >= .MinimumBaselineNoiseLevel
        With udtBaselineNoiseStats
            If .NoiseLevel < udtBaselineNoiseOptions.MinimumBaselineNoiseLevel AndAlso udtBaselineNoiseOptions.MinimumBaselineNoiseLevel > 0 Then
                .NoiseLevel = udtBaselineNoiseOptions.MinimumBaselineNoiseLevel
                .NoiseStDev = 0                             ' Set this to 0 since we have overridden .NoiseLevel
            End If
        End With

        Return True

    End Function

    Public Function ComputeNoiseLevel(ByRef sngData() As Single, ByVal intDataCount As Integer, ByVal udtBaselineNoiseOptions As udtBaselineNoiseOptionsType, ByRef udtBaselineNoiseStats As udtBaselineNoiseStatsType) As Boolean

        Const IGNORE_NON_POSITIVE_DATA As Boolean = True

        If udtBaselineNoiseOptions.BaselineNoiseMode = eNoiseThresholdModes.SegmentedTrimmedMedianByAbundance Then
            ' Segmented trimmed median is only valid when calling ComputeMultiSegmentNoiseLevel
            ' Auto-switch .BaselineNoiseMode to .TrimmedMedianByAbundance
            udtBaselineNoiseOptions.BaselineNoiseMode = eNoiseThresholdModes.TrimmedMedianByAbundance
        End If

        If udtBaselineNoiseOptions.BaselineNoiseMode = eNoiseThresholdModes.AbsoluteThreshold Then
            udtBaselineNoiseStats.NoiseLevel = udtBaselineNoiseOptions.BaselineNoiseLevelAbsolute
            Return True
        ElseIf udtBaselineNoiseOptions.BaselineNoiseMode = eNoiseThresholdModes.DualTrimmedMeanByAbundance Then
            Return ComputeDualTrimmedNoiseLevel(sngData, 0, intDataCount - 1, udtBaselineNoiseOptions, udtBaselineNoiseStats)
        Else
            ' Includes .TrimmedMeanByAbundance, .TrimmedMeanByCount, and .TrimmedMedianByAbundance
            Return ComputeTrimmedNoiseLevel(sngData, 0, intDataCount - 1, udtBaselineNoiseOptions, IGNORE_NON_POSITIVE_DATA, udtBaselineNoiseStats)
        End If
    End Function

    Public Function ComputeMultiSegmentNoiseLevel(ByRef sngX() As Single, ByRef sngY() As Single, ByVal intDataCount As Integer, ByVal udtBaselineNoiseOptions As udtBaselineNoiseOptionsType, ByRef udtBaselineNoiseStatSegments() As udtBaselineNoiseStatSegmentsType) As Boolean
        ' This function assumes sngX() is sorted ascending, and sngY() runs parallel to sngX()

        Const MINIMUM_SEGMENT_SIZE As Integer = 10

        Dim blnSegmentDone As Boolean
        Dim blnSuccess As Boolean

        Dim intMinimumPointsPerSegment As Integer
        Dim sngTargetSegmentWidthX As Single

        Dim sngSegmentIndexStart As Integer
        Dim sngSegmentIndexEnd As Integer
        Dim sngXEndThreshold As Single

        Dim intSegmentCount As Integer
        Dim intSegmentIndex As Integer

        Dim udtBaselineNoiseOptionsLocal As udtBaselineNoiseOptionsType

        Try

            intSegmentCount = 0
            ReDim udtBaselineNoiseStatSegments(9)

            If sngX Is Nothing OrElse sngY Is Nothing Then
                Return False
            ElseIf intDataCount < 2 Then
                ' Require at least 2 points to use this function
                Return False
            ElseIf sngX.Length < intDataCount Or sngY.Length < intDataCount Then
                ' Not enough data in sngX or sngY
                Return False
            End If

            ' Validate the segmenting settings
            intMinimumPointsPerSegment = udtBaselineNoiseOptions.SegmentedTrimmedMedianMinimumPointsPerSegment
            If intMinimumPointsPerSegment = 0 Then
                intMinimumPointsPerSegment = 50
            ElseIf intMinimumPointsPerSegment < MINIMUM_SEGMENT_SIZE Then
                intMinimumPointsPerSegment = MINIMUM_SEGMENT_SIZE
            End If

            sngTargetSegmentWidthX = udtBaselineNoiseOptions.SegmentedTrimmedMedianTargetSegmentWidthX
            If sngTargetSegmentWidthX = 0 Then
                sngTargetSegmentWidthX = 100
            ElseIf sngTargetSegmentWidthX < 1 Then
                sngTargetSegmentWidthX = 1
            End If

            ' Copy the values from udtBaselineNoiseOptions into a local copy, then override .BaselineNoiseMode
            udtBaselineNoiseOptionsLocal = udtBaselineNoiseOptions
            udtBaselineNoiseOptionsLocal.BaselineNoiseMode = eNoiseThresholdModes.TrimmedMedianByAbundance

            ' Define the segments
            sngSegmentIndexEnd = -1
            Do While sngSegmentIndexEnd < intDataCount - 1
                sngSegmentIndexStart = sngSegmentIndexEnd + 1

                sngXEndThreshold = sngX(sngSegmentIndexStart) + sngTargetSegmentWidthX
                sngSegmentIndexEnd = sngSegmentIndexStart + 1

                blnSegmentDone = False
                Do While Not blnSegmentDone
                    If sngSegmentIndexEnd + 1 >= intDataCount Then
                        ' No more points to add; segment is complete
                        blnSegmentDone = True
                    ElseIf sngX(sngSegmentIndexEnd + 1) < sngXEndThreshold Then
                        ' Segment not yet wide enough; add another point
                        sngSegmentIndexEnd += 1
                    ElseIf sngSegmentIndexEnd - sngSegmentIndexStart + 1 < intMinimumPointsPerSegment Then
                        ' Not enough points in this segment; add another point
                        sngSegmentIndexEnd += 1
                    Else
                        ' All conditions met; segment is complete
                        blnSegmentDone = True
                    End If
                Loop

                ' If less than MINIMUM_SEGMENT_SIZE points remain after sngSegmentIndexEnd then lump them into this segment
                '  to avoid having a tiny segment after this one
                If sngSegmentIndexEnd < intDataCount - 1 AndAlso intDataCount - (sngSegmentIndexEnd + 1) < MINIMUM_SEGMENT_SIZE Then
                    sngSegmentIndexEnd = intDataCount - 1
                End If

                If intSegmentCount >= udtBaselineNoiseStatSegments.Length Then
                    ' Reserve more space
                    ReDim Preserve udtBaselineNoiseStatSegments(udtBaselineNoiseStatSegments.Length * 2 - 1)
                End If

                With udtBaselineNoiseStatSegments(intSegmentCount)
                    InitializeBaselineNoiseStats(.BaselineNoiseStats, udtBaselineNoiseOptions.MinimumBaselineNoiseLevel, eNoiseThresholdModes.SegmentedTrimmedMedianByAbundance)
                    .SegmentIndexStart = sngSegmentIndexStart
                    .SegmentIndexEnd = sngSegmentIndexEnd
                    .ComputeMidPoint()
                End With

                intSegmentCount += 1
            Loop

            If intSegmentCount < udtBaselineNoiseStatSegments.Length Then
                ReDim Preserve udtBaselineNoiseStatSegments(intSegmentCount - 1)
            End If


            ' Call ComputeTrimmedNoiseLevel for each segment
            For intSegmentIndex = 0 To intSegmentCount - 1
                With udtBaselineNoiseStatSegments(intSegmentIndex)
                    blnSuccess = ComputeTrimmedNoiseLevel(sngY, .SegmentIndexStart, .SegmentIndexEnd, udtBaselineNoiseOptionsLocal, True, .BaselineNoiseStats)
                End With
            Next intSegmentIndex


        Catch ex As Exception
            Return False
        End Try

        Return True

    End Function

    Public Shared Function ComputeSignalToNoise(ByVal sngSignal As Single, ByVal sngNoiseThresholdIntensity As Single) As Single

        If sngNoiseThresholdIntensity > 0 Then
            Return sngSignal / sngNoiseThresholdIntensity
        Else
            Return 0
        End If

    End Function

    Public Shared Function ComputeSignalToNoiseUsingMultiSegmentData(ByVal intDataPointIndex As Integer, ByRef udtBaselineNoiseStatSegments() As udtBaselineNoiseStatSegmentsType) As Single
        ' This function assumes udtBaselineNoiseStatSegments() is sorted ascending

        Dim intSegmentIndexMax As Integer
        Dim intSegmentIndex As Integer

        Dim sngSlope As Single
        Dim sngNoiseLevel As Single

        sngNoiseLevel = 0
        If Not udtBaselineNoiseStatSegments Is Nothing AndAlso udtBaselineNoiseStatSegments.Length > 0 Then
            intSegmentIndexMax = udtBaselineNoiseStatSegments.Length - 1

            If intDataPointIndex <= udtBaselineNoiseStatSegments(0).SegmentMidpointValue Then
                sngNoiseLevel = udtBaselineNoiseStatSegments(0).BaselineNoiseStats.NoiseLevel
            ElseIf intDataPointIndex >= udtBaselineNoiseStatSegments(intSegmentIndexMax).SegmentMidpointValue Then
                sngNoiseLevel = udtBaselineNoiseStatSegments(intSegmentIndexMax).BaselineNoiseStats.NoiseLevel
            Else
                ' Find the segment in which sngX resides
                For intSegmentIndex = 0 To udtBaselineNoiseStatSegments.Length - 1
                    With udtBaselineNoiseStatSegments(intSegmentIndex)
                        If intDataPointIndex >= .SegmentIndexStart Then
                            If intDataPointIndex <= .SegmentIndexEnd Then
                                ' Match Found
                                ' See if sngX is before or after the midpoint of the this segment
                                If intDataPointIndex <= .SegmentMidpointValue Then
                                    ' sngX is before or at the midpoint
                                    If intSegmentIndex = 0 Then
                                        sngNoiseLevel = .BaselineNoiseStats.NoiseLevel
                                    Else
                                        ' sngNoiseLevel = Slope * (x - x0) + y0
                                        sngSlope = (.BaselineNoiseStats.NoiseLevel - udtBaselineNoiseStatSegments(intSegmentIndex - 1).BaselineNoiseStats.NoiseLevel) / (.SegmentMidpointValue - udtBaselineNoiseStatSegments(intSegmentIndex - 1).SegmentMidpointValue)
                                        sngNoiseLevel = sngSlope * (intDataPointIndex - .SegmentMidpointValue) + .BaselineNoiseStats.NoiseLevel
                                    End If
                                Else
                                    ' sngX is after the midpoint
                                    If intSegmentIndex = udtBaselineNoiseStatSegments.Length - 1 Then
                                        sngNoiseLevel = .BaselineNoiseStats.NoiseLevel
                                    Else
                                        ' sngNoiseLevel = Slope * (x - x0) + y0
                                        sngSlope = (udtBaselineNoiseStatSegments(intSegmentIndex + 1).BaselineNoiseStats.NoiseLevel - .BaselineNoiseStats.NoiseLevel) / (udtBaselineNoiseStatSegments(intSegmentIndex + 1).SegmentMidpointValue - .SegmentMidpointValue)
                                        sngNoiseLevel = sngSlope * (intDataPointIndex - .SegmentMidpointValue) + .BaselineNoiseStats.NoiseLevel
                                    End If
                                End If
                                Exit For
                            End If
                        End If

                    End With
                Next intSegmentIndex

            End If

        End If

        Return sngNoiseLevel

    End Function

    Protected Function ComputeTrimmedNoiseLevel(ByRef sngData() As Single, ByVal intIndexStart As Integer, ByVal intIndexEnd As Integer, ByVal udtBaselineNoiseOptions As udtBaselineNoiseOptionsType, ByVal blnIgnoreNonPositiveData As Boolean, ByRef udtBaselineNoiseStats As udtBaselineNoiseStatsType) As Boolean
        ' Computes a trimmed mean or trimmed median using the low intensity data up to udtBaselineNoiseOptions.TrimmedMeanFractionLowIntensityDataToExamine
        ' Additionally, computes a full median using all data in sngData
        ' If blnIgnoreNonPositiveData is True, then removes data from sngData() <= 0 and <= .MinimumBaselineNoiseLevel
        ' Returns True if success, False if error (or no data in sngData)

        ' Note: Replaces values of 0 with the minimum positive value in sngData()
        ' Note: You cannot use sngData.Length to determine the length of the array; use intDataCount

        Dim intDataSortedCount As Integer
        Dim sngDataSorted() As Single           ' Note: You cannot use sngDataSorted.Length to determine the length of the array; use intIndexStart and intIndexEnd to find the limits
        Dim sngMinimumPositiveValue As Single

        Dim dblIntensityThreshold As Double
        Dim dblSum As Double

        Dim intIndex As Integer
        Dim intValidDataCount As Integer

        Dim intCountSummed As Integer

        ' Initialize udtBaselineNoiseStats
        InitializeBaselineNoiseStats(udtBaselineNoiseStats, udtBaselineNoiseOptions.MinimumBaselineNoiseLevel, udtBaselineNoiseOptions.BaselineNoiseMode)

        If sngData Is Nothing OrElse intIndexEnd - intIndexStart < 0 Then
            Return False
        End If

        ' Copy the data into sngDataSorted
        intDataSortedCount = intIndexEnd - intIndexStart + 1
        ReDim sngDataSorted(intDataSortedCount - 1)

        If intIndexStart = 0 Then
            Array.Copy(sngData, sngDataSorted, intDataSortedCount)
        Else
            For intIndex = intIndexStart To intIndexEnd
                sngDataSorted(intIndex - intIndexStart) = sngData(intIndex)
            Next intIndex
        End If

        ' Sort the array
        Array.Sort(sngDataSorted)

        If blnIgnoreNonPositiveData Then
            ' Remove data with a value <= 0 

            If sngDataSorted(0) <= 0 Then
                intValidDataCount = 0
                For intIndex = 0 To intDataSortedCount - 1
                    If sngDataSorted(intIndex) > 0 Then
                        sngDataSorted(intValidDataCount) = sngDataSorted(intIndex)
                        intValidDataCount += 1
                    End If
                Next intIndex

                If intValidDataCount < intDataSortedCount Then
                    intDataSortedCount = intValidDataCount
                End If

                ' Check for no data remaining
                If intDataSortedCount <= 0 Then
                    Return False
                End If
            End If
        End If

        ' Look for the minimum positive value and replace all data in sngDataSorted with that value
        sngMinimumPositiveValue = ReplaceSortedDataWithMinimumPositiveValue(intDataSortedCount, sngDataSorted)

        Select Case udtBaselineNoiseOptions.BaselineNoiseMode
            Case eNoiseThresholdModes.TrimmedMeanByAbundance, eNoiseThresholdModes.TrimmedMeanByCount

                If udtBaselineNoiseOptions.BaselineNoiseMode = eNoiseThresholdModes.TrimmedMeanByAbundance Then
                    ' Average the data that has intensity values less than
                    '  Minimum + udtBaselineNoiseOptions.TrimmedMeanFractionLowIntensityDataToExamine * (Maximum - Minimum)
                    With udtBaselineNoiseOptions
                        dblIntensityThreshold = sngDataSorted(0) + .TrimmedMeanFractionLowIntensityDataToExamine * (sngDataSorted(intDataSortedCount - 1) - sngDataSorted(0))
                    End With

                    ' Initialize intCountSummed to intDataSortedCount for now, in case all data is within the intensity threshold
                    intCountSummed = intDataSortedCount
                    dblSum = 0
                    For intIndex = 0 To intDataSortedCount - 1
                        If sngDataSorted(intIndex) <= dblIntensityThreshold Then
                            dblSum += sngDataSorted(intIndex)
                        Else
                            ' Update intCountSummed
                            intCountSummed = intIndex
                            Exit For
                        End If
                    Next intIndex
                    intIndexEnd = intCountSummed - 1
                Else
                    ' eNoiseThresholdModes.TrimmedMeanByCount
                    ' Find the index of the data point at intDataSortedCount * udtBaselineNoiseOptions.TrimmedMeanFractionLowIntensityDataToExamine and
                    '  average the data from the start to that index
                    intIndexEnd = CInt(Math.Round((intDataSortedCount - 1) * udtBaselineNoiseOptions.TrimmedMeanFractionLowIntensityDataToExamine, 0))

                    intCountSummed = intIndexEnd + 1
                    dblSum = 0
                    For intIndex = 0 To intIndexEnd
                        dblSum += sngDataSorted(intIndex)
                    Next intIndex
                End If

                If intCountSummed > 0 Then
                    ' Compute the average
                    ' Note that intCountSummed will be used below in the variance computation
                    With udtBaselineNoiseStats
                        .NoiseLevel = CSng(dblSum / intCountSummed)
                        .PointsUsed = intCountSummed
                    End With

                    If intCountSummed > 1 Then
                        ' Compute the variance
                        dblSum = 0
                        For intIndex = 0 To intIndexEnd
                            dblSum += (sngDataSorted(intIndex) - udtBaselineNoiseStats.NoiseLevel) ^ 2
                        Next intIndex
                        udtBaselineNoiseStats.NoiseStDev = CSng(Math.Sqrt(dblSum / (intCountSummed - 1)))
                    Else
                        udtBaselineNoiseStats.NoiseStDev = 0
                    End If
                Else
                    ' No data to average; define the noise level to be the minimum intensity
                    With udtBaselineNoiseStats
                        .NoiseLevel = sngDataSorted(0)
                        .NoiseStDev = 0
                        .PointsUsed = 1
                    End With
                End If

            Case eNoiseThresholdModes.TrimmedMedianByAbundance
                If udtBaselineNoiseOptions.TrimmedMeanFractionLowIntensityDataToExamine >= 1 Then
                    intIndexEnd = intDataSortedCount - 1
                Else
                    'Find the median of the data that has intensity values less than
                    '  Minimum + udtBaselineNoiseOptions.TrimmedMeanFractionLowIntensityDataToExamine * (Maximum - Minimum)
                    With udtBaselineNoiseOptions
                        dblIntensityThreshold = sngDataSorted(0) + .TrimmedMeanFractionLowIntensityDataToExamine * (sngDataSorted(intDataSortedCount - 1) - sngDataSorted(0))
                    End With

                    ' Find the first point with an intensity value <= dblIntensityThreshold
                    intIndexEnd = intDataSortedCount - 1
                    For intIndex = 1 To intDataSortedCount - 1
                        If sngDataSorted(intIndex) > dblIntensityThreshold Then
                            intIndexEnd = intIndex - 1
                            Exit For
                        End If
                    Next intIndex
                End If

                If intIndexEnd Mod 2 = 0 Then
                    ' Even value
                    udtBaselineNoiseStats.NoiseLevel = sngDataSorted(CInt(intIndexEnd / 2))
                Else
                    ' Odd value; average the values on either side of intIndexEnd/2
                    intIndex = CInt((intIndexEnd - 1) / 2)
                    If intIndex < 0 Then intIndex = 0
                    dblSum = sngDataSorted(intIndex)

                    intIndex += 1
                    If intIndex = intDataSortedCount Then intIndex = intDataSortedCount - 1
                    dblSum += sngDataSorted(intIndex)

                    udtBaselineNoiseStats.NoiseLevel = CSng(dblSum / 2.0)
                End If

                ' Compute the variance
                dblSum = 0
                For intIndex = 0 To intIndexEnd
                    dblSum += (sngDataSorted(intIndex) - udtBaselineNoiseStats.NoiseLevel) ^ 2
                Next intIndex

                With udtBaselineNoiseStats
                    intCountSummed = intIndexEnd + 1
                    If intCountSummed > 0 Then
                        .NoiseStDev = CSng(Math.Sqrt(dblSum / (intCountSummed - 1)))
                    Else
                        .NoiseStDev = 0
                    End If
                    .PointsUsed = intCountSummed
                End With
            Case Else
                ' Unknown mode
                Throw New Exception("clsBaselineNoiseEstimator->ComputeTrimmedNoiseLevel; Unknown Noise Threshold Mode encountered: " & udtBaselineNoiseOptions.BaselineNoiseMode.ToString)
                Return False
        End Select

        ' Assure that .NoiseLevel is >= .MinimumBaselineNoiseLevel
        With udtBaselineNoiseStats
            If .NoiseLevel < udtBaselineNoiseOptions.MinimumBaselineNoiseLevel AndAlso udtBaselineNoiseOptions.MinimumBaselineNoiseLevel > 0 Then
                .NoiseLevel = udtBaselineNoiseOptions.MinimumBaselineNoiseLevel
                .NoiseStDev = 0                             ' Set this to 0 since we have overridden .NoiseLevel
            End If
        End With

        Return True

    End Function

    Public Shared Sub InitializeBaselineNoiseStats(ByRef udtBaselineNoiseStats As udtBaselineNoiseStatsType, ByVal sngMinimumBaselineNoiseLevel As Single, ByVal eNoiseThresholdMode As eNoiseThresholdModes)
        With udtBaselineNoiseStats
            .NoiseLevel = sngMinimumBaselineNoiseLevel
            .NoiseStDev = 0
            .PointsUsed = 0
            .NoiseThresholdModeUsed = eNoiseThresholdMode
        End With
    End Sub

    Private Function ReplaceSortedDataWithMinimumPositiveValue(ByVal intDataCount As Integer, ByRef sngDataSorted() As Single) As Single
        ' This function assumes sngDataSorted() is sorted ascending
        ' It looks for the minimum positive value in sngDataSorted() and returns that value
        ' Additionally, it replaces all values of 0 in sngDataSorted() with sngMinimumPositiveValue

        Dim sngMinimumPositiveValue As Single
        Dim intIndex As Integer
        Dim intIndexFirstPositiveValue As Integer

        ' Find the minimum positive value in sngDataSorted
        ' Since it's sorted, we can stop at the first non-zero value

        intIndexFirstPositiveValue = -1
        sngMinimumPositiveValue = 0
        For intIndex = 0 To intDataCount - 1
            If sngDataSorted(intIndex) > 0 Then
                intIndexFirstPositiveValue = intIndex
                sngMinimumPositiveValue = sngDataSorted(intIndex)
                Exit For
            End If
        Next intIndex

        If sngMinimumPositiveValue < 1 Then sngMinimumPositiveValue = 1
        For intIndex = intIndexFirstPositiveValue To 0 Step -1
            sngDataSorted(intIndex) = sngMinimumPositiveValue
        Next intIndex

        Return sngMinimumPositiveValue

    End Function

    Private Function TestSignificanceUsingTTest(ByVal dblMean1 As Double, ByVal dblMean2 As Double, ByVal dblStDev1 As Double, ByVal dblStDev2 As Double, ByVal intCount1 As Integer, ByVal intCount2 As Integer, ByVal eConfidenceLevel As eTTestConfidenceLevelConstants, ByRef TCalculated As Double) As Boolean
        ' Uses the means and sigma values to compute the t-test value between the two populations to determine if they are statistically different
        ' To use the t-test you must use sample variance values, not population variance values
        ' Note: Variance_Sample = Sum((x-mean)^2) / (count-1)
        ' Note: Sigma = SquareRoot(Variance_Sample)
        '
        ' Returns True if the two populations are statistically different, based on the given significance threshold

        ' Significance Table:
        ' Confidence Levels and critical values:
        ' 80%, 90%, 95%, 98%, 99%, 99.5%, 99.8%, 99.9%
        ' 1.886, 2.920, 4.303, 6.965, 9.925, 14.089, 22.327, 31.598

        Static ConfidenceLevels() As Single = New Single() {1.886, 2.92, 4.303, 6.965, 9.925, 14.089, 22.327, 31.598}

        Dim SPooled As Double
        Dim intConfidenceLevelIndex As Integer

        If intCount1 + intCount2 <= 2 Then
            ' Cannot compute the T-Test
            TCalculated = 0
            Return False
        Else

            SPooled = Math.Sqrt(((dblStDev1 ^ 2) * (intCount1 - 1) + (dblStDev2 ^ 2) * (intCount2 - 1)) / (intCount1 + intCount2 - 2))
            TCalculated = ((dblMean1 - dblMean2) / SPooled) * Math.Sqrt(intCount1 * intCount2 / (intCount1 + intCount2))

            intConfidenceLevelIndex = eConfidenceLevel
            If intConfidenceLevelIndex < 0 Then
                intConfidenceLevelIndex = 0
            ElseIf intConfidenceLevelIndex >= ConfidenceLevels.Length Then
                intConfidenceLevelIndex = ConfidenceLevels.Length - 1
            End If

            If TCalculated >= ConfidenceLevels(intConfidenceLevelIndex) Then
                ' Differences are significant
                Return True
            Else
                ' Differences are not significant
                Return False
            End If
        End If

    End Function
End Class
