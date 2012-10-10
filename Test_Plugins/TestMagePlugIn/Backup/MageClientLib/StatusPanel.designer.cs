namespace MageClientLib {
    partial class StatusPanel {
        /// <summary> 
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary> 
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing) {
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
        private void InitializeComponent() {
            this.panel1 = new System.Windows.Forms.Panel();
            this.StatusMessageCtl = new System.Windows.Forms.Label();
            this.CancelCtl = new System.Windows.Forms.Button();
            this.panel1.SuspendLayout();
            this.SuspendLayout();
            // 
            // panel1
            // 
            this.panel1.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.panel1.Controls.Add(this.CancelCtl);
            this.panel1.Controls.Add(this.StatusMessageCtl);
            this.panel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.panel1.Location = new System.Drawing.Point(5, 5);
            this.panel1.Name = "panel1";
            this.panel1.Size = new System.Drawing.Size(442, 32);
            this.panel1.TabIndex = 0;
            // 
            // StatusMessageCtl
            // 
            this.StatusMessageCtl.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left)
                        | System.Windows.Forms.AnchorStyles.Right)));
            this.StatusMessageCtl.AutoEllipsis = true;
            this.StatusMessageCtl.AutoSize = true;
            this.StatusMessageCtl.Location = new System.Drawing.Point(3, 8);
            this.StatusMessageCtl.Name = "StatusMessageCtl";
            this.StatusMessageCtl.Size = new System.Drawing.Size(41, 13);
            this.StatusMessageCtl.TabIndex = 1;
            this.StatusMessageCtl.Text = "(status)";
            // 
            // CancelCtl
            // 
            this.CancelCtl.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.CancelCtl.Location = new System.Drawing.Point(362, 3);
            this.CancelCtl.Name = "CancelCtl";
            this.CancelCtl.Size = new System.Drawing.Size(75, 23);
            this.CancelCtl.TabIndex = 0;
            this.CancelCtl.Text = "Cancel";
            this.CancelCtl.UseVisualStyleBackColor = true;
            this.CancelCtl.Click += new System.EventHandler(this.CancelCtl_Click);
            // 
            // StatusPanel
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.Controls.Add(this.panel1);
            this.Name = "StatusPanel";
            this.Padding = new System.Windows.Forms.Padding(5);
            this.Size = new System.Drawing.Size(452, 42);
            this.panel1.ResumeLayout(false);
            this.panel1.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Panel panel1;
        private System.Windows.Forms.Button CancelCtl;
        private System.Windows.Forms.Label StatusMessageCtl;
    }
}
