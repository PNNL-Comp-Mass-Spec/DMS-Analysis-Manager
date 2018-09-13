using System;
using System.Collections.Generic;
using System.IO;
using PdfSharp.Drawing;
using PdfSharp.Pdf;

namespace AnalysisManagerFormularityPlugin
{
    public class PngToPdfConverter : PRISM.clsEventNotifier
    {
        /// <summary>
        /// Page margin, in points
        /// </summary>
        /// <remarks>
        /// For reference: 72 points is 1 inch, as defined by the DTP (or PostScript) point.
        /// </remarks>
        private const double PageMargin = 36;

        private const double DoublePageMargin = PageMargin * 2;

        private readonly PdfDocument mDocument;
        private PdfPage mCurrentPage;
        private XGraphics mCurrentPageGraphics;

        private readonly XFont mFontHeader;
        private readonly XFont mFontDefault;

        private double currentPageY;

        /// <summary>
        /// Dataset name
        /// </summary>
        private string DatasetName { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="datasetName"></param>
        public PngToPdfConverter(string datasetName)
        {
            DatasetName = datasetName;

            const string FONT_FAMILY = "Arial";
            mFontHeader = new XFont(FONT_FAMILY, 20, XFontStyle.Regular);
            mFontDefault = new XFont(FONT_FAMILY, 10, XFontStyle.Regular);

            mDocument = new PdfDocument();
        }

        private void AddPage()
        {
            mCurrentPage = mDocument.AddPage();
            //page.Size = ;
            mCurrentPageGraphics = XGraphics.FromPdfPage(mCurrentPage);
            currentPageY = PageMargin;
        }

        /// <summary>
        /// Adds a plot image
        /// </summary>
        /// <param name="plotImage">Plot image</param>
        /// <param name="xOffset">X offset</param>
        /// <param name="width">Plot width, in points</param>
        /// <param name="height">Plot height, in points</param>
        /// <param name="header">Header (optional)</param>
        /// <returns></returns>
        private double AddPlot(XImage plotImage, double xOffset, double width, double height, string header = null)
        {
            var yPosition = currentPageY;
            if (!string.IsNullOrWhiteSpace(header))
            {
                yPosition += AddText(header, mFontDefault, x: xOffset) - 10;
            }

            mCurrentPageGraphics.DrawImage(plotImage, new XRect(xOffset, yPosition, width, height));
            plotImage.Dispose();

            return yPosition - currentPageY + height;
        }

        private double AddText(string text, XFont font, double yOffset = 0, double x = 0, double width = -1, XStringFormat position = null)
        {
            var textHeight = GetTextHeight(text, mCurrentPageGraphics, font);
            var y = currentPageY;

            if (!yOffset.Equals(0))
            {
                y += textHeight * yOffset;
            }

            if (x < PageMargin || x > mCurrentPage.Width - PageMargin)
            {
                x = PageMargin;
            }

            if (width < x || width > mCurrentPage.Width - DoublePageMargin)
            {
                width = mCurrentPage.Width - DoublePageMargin;
            }

            if (position == null)
            {
                position = XStringFormats.Default;
            }

            var height = textHeight;
            if (position.LineAlignment == XLineAlignment.BaseLine)
            {
                height = 0;
            }

            mCurrentPageGraphics.DrawString(text, font, XBrushes.Black, new XRect(x, y, width, height));

            return y - currentPageY + textHeight;
        }

        /// <summary>
        /// Create a PDF file with the given PNG files
        /// </summary>
        /// <param name="pdfFilePath">Path to the PDF file to be created</param>
        /// <param name="pngFiles"></param>
        /// <param name="dataSource"></param>
        /// <returns></returns>
        public bool CreatePdf(string pdfFilePath, List<FileInfo> pngFiles, string dataSource = "NOMSI")
        {
            if (string.IsNullOrWhiteSpace(dataSource))
            {
                dataSource = "Unknown_Source";
            }

            try
            {

                var plotSizeMultiplier = 4d;

                // Guarantee that the plots will be backed at 300+ DPI
                var desiredResolution = 300;
                var resolution = 96;

                // Add a page (this initializes currentPageY)
                AddPage();

                // Add a header: Dataset name
                currentPageY += AddText(DatasetName, mFontHeader, 0.75, position: XStringFormats.BottomCenter);


                // Add each of the plots

                var datasetDetailReportLink = "";
                var pngFileTableLayout = GetPngFileTableLayout(DatasetName, datasetDetailReportLink);

                var pngFileNames = new SortedSet<string>();
                foreach (var item in pngFiles)
                {
                    pngFileNames.Add(item.Name);
                }


                var plotHeight = (mCurrentPage.Width - DoublePageMargin - 10) / 3;

                const int xOffsetIncrement = 300;

                foreach (var tableRow in pngFileTableLayout)
                {
                    var xOffset = PageMargin;
                    double yOffsetIncrement = 0;

                    foreach (var tableCell in tableRow)
                    {
                        if (!string.IsNullOrWhiteSpace(tableCell))
                        {

                            if (pngFileNames.Contains(tableCell))
                            {
                                var pngFilePath = tableCell;

                                // Obsolete: var textHeight = GetTextHeight("int", mCurrentPageGraphics, mFontDefault);

                                var plotImage = XImage.FromFile(pngFilePath);

                                var plotWidth = plotHeight;

                                // Note that AddPlot will call plotImage.Dispose
                                AddPlot(plotImage, xOffset, plotImage.PointHeight, plotImage.PointWidth, "");
                                xOffset += plotWidth + 5;

                                yOffsetIncrement = Math.Max(yOffsetIncrement, plotHeight);
                            }
                            else
                            {
                                var textHeight = AddText(string.Format("File not found: {0}", tableCell), mFontDefault, 0, xOffset);

                                yOffsetIncrement = Math.Max(yOffsetIncrement, textHeight);
                            }
                        }

                        xOffset += xOffsetIncrement;
                    }

                    currentPageY += yOffsetIncrement;
                }

                return true;
            }
            catch (Exception ex)
            {
                base.OnErrorEvent("Error creating the PDF using .png files from " + dataSource, ex);
                return false;
            }

        }

        public static List<List<string>> GetPngFileTableLayout(string datasetName, string datasetDetailReportLink)
        {

            // PNG filename suffix
            var suffix = "_" + datasetName + ".png";

            // This tracks the PNG file names that will be linked to in the HTML table
            // There are two .png images in each row of the table
            // Use "text: " to instead include literal HTML text between <td> and </td>
            var tableLayoutByRow = new List<List<string>>
            {
                new List<string> {"mErr" + suffix, "histA" + suffix},
                new List<string> {"KMD1_assigned" + suffix, "histM" + suffix},
                new List<string> {"KMD1_unassigned" + suffix, datasetDetailReportLink},
                new List<string> {"vK" + suffix, "EC_count" + suffix},
                new List<string> {"Ox" + suffix, "OxN" + suffix},
                new List<string> {"OxS" + suffix, "OxP" + suffix}
            };

            return tableLayoutByRow;
        }

        private static double GetTextHeight(string text, XGraphics gfx, XFont font)
        {
            var textSize = gfx.MeasureString(text, font);
            return textSize.Height;
        }
    }
}
