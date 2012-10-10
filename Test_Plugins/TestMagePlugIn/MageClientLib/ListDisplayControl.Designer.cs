namespace MageClientLib {
    partial class ListDisplayControl {
        /// <summary> 
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary> 
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null)) {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Component Designer generated code

        /// <summary> 
        /// Required method for Designer support - do not modify 
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.lvQueryResults = new System.Windows.Forms.ListView();
            this.panel1 = new System.Windows.Forms.Panel();
            this.lblPageTitle = new System.Windows.Forms.Label();
            this.lblNotice = new System.Windows.Forms.Label();
            this.panel1.SuspendLayout();
            this.SuspendLayout();
            // 
            // lvQueryResults
            // 
            this.lvQueryResults.Dock = System.Windows.Forms.DockStyle.Fill;
            this.lvQueryResults.FullRowSelect = true;
            this.lvQueryResults.GridLines = true;
            this.lvQueryResults.HideSelection = false;
            this.lvQueryResults.Location = new System.Drawing.Point(0, 28);
            this.lvQueryResults.Name = "lvQueryResults";
            this.lvQueryResults.Size = new System.Drawing.Size(712, 518);
            this.lvQueryResults.TabIndex = 3;
            this.lvQueryResults.UseCompatibleStateImageBehavior = false;
            this.lvQueryResults.View = System.Windows.Forms.View.Details;
            this.lvQueryResults.ColumnClick += new System.Windows.Forms.ColumnClickEventHandler(this.lvQueryResults_ColumnClicked);
            // 
            // panel1
            // 
            this.panel1.Controls.Add(this.lblPageTitle);
            this.panel1.Controls.Add(this.lblNotice);
            this.panel1.Dock = System.Windows.Forms.DockStyle.Top;
            this.panel1.Location = new System.Drawing.Point(0, 0);
            this.panel1.Name = "panel1";
            this.panel1.Size = new System.Drawing.Size(712, 28);
            this.panel1.TabIndex = 4;
            // 
            // lblPageTitle
            // 
            this.lblPageTitle.AutoSize = true;
            this.lblPageTitle.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblPageTitle.Location = new System.Drawing.Point(3, 5);
            this.lblPageTitle.Margin = new System.Windows.Forms.Padding(3, 5, 3, 0);
            this.lblPageTitle.Name = "lblPageTitle";
            this.lblPageTitle.Size = new System.Drawing.Size(32, 13);
            this.lblPageTitle.TabIndex = 5;
            this.lblPageTitle.Text = "Title";
            // 
            // lblNotice
            // 
            this.lblNotice.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left)
                        | System.Windows.Forms.AnchorStyles.Right)));
            this.lblNotice.Location = new System.Drawing.Point(381, 5);
            this.lblNotice.Name = "lblNotice";
            this.lblNotice.Size = new System.Drawing.Size(328, 18);
            this.lblNotice.TabIndex = 6;
            this.lblNotice.TextAlign = System.Drawing.ContentAlignment.TopRight;
            // 
            // ListDisplayControl
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.Controls.Add(this.lvQueryResults);
            this.Controls.Add(this.panel1);
            this.Name = "ListDisplayControl";
            this.Size = new System.Drawing.Size(712, 546);
            this.panel1.ResumeLayout(false);
            this.panel1.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.ListView lvQueryResults;
        private System.Windows.Forms.Panel panel1;
        private System.Windows.Forms.Label lblPageTitle;
        private System.Windows.Forms.Label lblNotice;
    }
}
