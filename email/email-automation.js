// email/email-automation.js - SalesHandy-style Manual Email Sequence Management with Email Rotation
require('dotenv').config();
const nodemailer = require('nodemailer');
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

class SalesHandyEmailSystem {
    constructor() {
        // PostgreSQL connection
        this.pool = new Pool({
            connectionString: process.env.DATABASE_URL,
            ssl: { rejectUnauthorized: false }
        });

        // Email rotation tracking
        this.transporters = new Map(); // Map de sequenceId -> transporters array
        this.rotationIndexes = new Map(); // Map de sequenceId -> current index
        
        // Email templates (Javier's content)
        this.emailTemplates = {
            day_1: {
                subject: "Quick automation question for {{company}}",
                body: `{spin}Hi|Hello|Hey there{endspin} {{name}},

Hope you're doing well. I wanted to reach out because we help {{industry}} teams like yours save hours every week by replacing time-consuming manual tasks with smart, AI-powered automations.

At Tribearium Solutions, we do more than just connect systems, we partner with your team to design automations that fit the way your business actually runs. Whether it's capturing leads, onboarding clients, or handling admin workflows, we tailor every solution to free up time, reduce errors, and help your team focus on what matters most.

To show you how this could work for your business, we're offering a free personalized demo. All we need is 3 minutes of your time to understand your workflows.

Our demos have helped founders:
• Cut down client onboarding time by 50%
• Automate internal requests & approvals
• Get visibility into ops with a live dashboard

Fill out this short form https://tally.so/r/w8BL6Y and we'll send you a custom automation or MVP prototype, no strings attached.

{spin}Best regards,|Sincerely,|Regards{endspin}
Javier

https://calendly.com/tribeariumsolutions/30min`
            },
            
            day_3: {
                subject: "Re: Automation for {{company}}",
                body: `Hi {{name}},

Just wanted to follow up in case my note got buried.

If saving time by automating lead capture, onboarding, or admin work is something you're considering, I'd be happy to show you what that could look like for your team.

We'd like to send you a quick proposal or even a free automation prototype; we just need a bit more information to tailor it.

You can:
• Book a 15-min call https://calendly.com/tribeariumsolutions/30min
• Fill out this short form https://tally.so/r/w8BL6Y
• Or just reply with what you're looking to improve

No pressure, we're happy to help if the timing's right.

Let me know what works best,
Javier`
            },
            
            day_7: {
                subject: "One question for {{name}} at {{company}}",
                body: `Hi {{name}},

If there's one manual task your team would love to get off their plate, what would it be?

We've helped other {{industry}} teams automate everything from intake forms to backend operations—and it's usually easier than it sounds.

If you'd like, I can take a quick look and let you know what's possible, no strings attached.

Best,
Javier

https://calendly.com/tribeariumsolutions/30min`
            },
            
            day_9: {
                subject: "Final note for {{name}} at {{company}}",
                body: `Hi {{name}},

I haven't heard back, so I'll assume timing might not be right. Totally understand.

If automation becomes a priority down the line, feel free to reach out. Always happy to help you identify ways to free up time and reduce busywork.

Wishing you and your team continued success,
Javier

https://calendly.com/tribeariumsolutions/30min`
            }
        };

        // Default SMTP configuration
        this.setupDefaultSMTP();
    }

    setupDefaultSMTP() {
        if (process.env.GMAIL_USER && process.env.GMAIL_APP_PASSWORD) {
            const transportConfig = {
                service: 'gmail',
                auth: {
                    user: process.env.GMAIL_USER,
                    pass: process.env.GMAIL_APP_PASSWORD
                }
            };

            if (process.env.STREAM_TRANSPORT === 'true') {
                transportConfig.streamTransport = true;
                console.log('🔧 Stream Transport enabled for Default SMTP');
            }

            this.defaultTransporter = nodemailer.createTransport(transportConfig);
            this.transporter = this.defaultTransporter;
            console.log('✅ Default Gmail SMTP configured');
        }
    }

    // =====================
    // EMAIL ROTATION MANAGEMENT
    // =====================
    
    async createTransportersForSequence(sequenceId, senderEmails) {
        const transporters = [];
        
        for (const account of senderEmails) {
            try {
                const transportConfig = {
                    service: 'gmail',
                    auth: {
                        user: account.email,
                        pass: account.password
                    }
                };

                if (process.env.STREAM_TRANSPORT === 'true') {
                    transportConfig.streamTransport = true;
                    console.log(`🔧 Stream Transport enabled for ${account.email}`);
                }

                const transporter = nodemailer.createTransport(transportConfig);
                
                // Verificar que el transporter funciona
                if (process.env.STREAM_TRANSPORT !== 'true') {
                    await transporter.verify();
                }

                transporters.push({
                    email: account.email,
                    transporter: transporter,
                    dailySent: 0,
                    lastResetDate: new Date().toDateString()
                });
                
                console.log(`✅ Transporter configurado para ${account.email}`);
            } catch (error) {
                console.error(`❌ Error configurando ${account.email}:`, error.message);
            }
        }
        
        if (transporters.length > 0) {
            this.transporters.set(sequenceId, transporters);
            this.rotationIndexes.set(sequenceId, 0);
            console.log(`📧 ${transporters.length} transporters listos para secuencia ${sequenceId}`);
        }
        
        return transporters;
    }
    
    getNextTransporter(sequenceId, distributionMethod = 'round-robin') {
        const transporters = this.transporters.get(sequenceId);
        
        if (!transporters || transporters.length === 0) {
            return {
                email: process.env.GMAIL_USER,
                transporter: this.defaultTransporter
            };
        }
        
        // Reset daily counters if it's a new day
        const today = new Date().toDateString();
        transporters.forEach(t => {
            if (t.lastResetDate !== today) {
                t.dailySent = 0;
                t.lastResetDate = today;
            }
        });
        
        let selectedTransporter;
        
        switch (distributionMethod) {
            case 'random':
                const availableTransporters = transporters.filter(t => t.dailySent < 50);
                if (availableTransporters.length === 0) {
                    console.warn('⚠️ Todos los transporters alcanzaron el límite diario');
                    return null;
                }
                const randomIndex = Math.floor(Math.random() * availableTransporters.length);
                selectedTransporter = availableTransporters[randomIndex];
                break;
                
            case 'weighted':
                // Seleccionar el transporter con menos emails enviados hoy
                const sortedTransporters = [...transporters].sort((a, b) => a.dailySent - b.dailySent);
                selectedTransporter = sortedTransporters.find(t => t.dailySent < 50);
                break;
                
            case 'round-robin':
            default:
                let currentIndex = this.rotationIndexes.get(sequenceId) || 0;
                let attempts = 0;
                
                while (attempts < transporters.length) {
                    selectedTransporter = transporters[currentIndex % transporters.length];
                    if (selectedTransporter.dailySent < 50) {
                        break;
                    }
                    currentIndex++;
                    attempts++;
                }
                
                if (attempts === transporters.length && selectedTransporter.dailySent >= 50) {
                    console.warn('⚠️ Todos los transporters alcanzaron el límite diario');
                    return null;
                }
                
                this.rotationIndexes.set(sequenceId, (currentIndex + 1) % transporters.length);
                break;
        }
        
        if (selectedTransporter) {
            selectedTransporter.dailySent++;
        }
        
        return selectedTransporter;
    }

    // =====================
    // SEQUENCE MANAGEMENT WITH ROTATION
    // =====================

    async createSequence(sequenceData) {
        try {
            console.log('Creating new sequence with email rotation:', sequenceData.name);
            
            const { 
                name, 
                description, 
                sender_emails,
                distribution_method,
                daily_limit_per_email,
                templates 
            } = sequenceData;
            
            const sequenceId = Date.now().toString();
            
            // Preparar datos de remitentes
            const senderEmailsData = sender_emails || [{
                email: process.env.GMAIL_USER,
                password: process.env.GMAIL_APP_PASSWORD
            }];
            
            // Crear la secuencia en la base de datos
            await this.pool.query(`
                INSERT INTO sequences (
                    id, name, description, templates, status, created_by,
                    sender_emails, distribution_method, daily_limit_per_email, email_rotation_index,
                    created_at, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 0, NOW(), NOW())
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    sender_emails = EXCLUDED.sender_emails,
                    distribution_method = EXCLUDED.distribution_method,
                    daily_limit_per_email = EXCLUDED.daily_limit_per_email,
                    updated_at = NOW()
            `, [
                sequenceId, 
                name, 
                description || '', 
                JSON.stringify(templates || ['day_1', 'day_3', 'day_7', 'day_9']),
                'draft',
                'Tribearium',
                JSON.stringify(senderEmailsData),
                distribution_method || 'round-robin',
                daily_limit_per_email || 50
            ]);
            
            // Crear transporters para esta secuencia
            await this.createTransportersForSequence(sequenceId, senderEmailsData);
            
            console.log(`✅ Secuencia creada: ${name} (ID: ${sequenceId}) con ${senderEmailsData.length} remitente(s)`);
            
            return {
                id: sequenceId,
                name: name,
                description: description,
                sender_emails_count: senderEmailsData.length,
                distribution_method: distribution_method || 'round-robin',
                daily_limit_per_email: daily_limit_per_email || 50,
                status: 'draft',
                created_at: new Date().toISOString()
            };
            
        } catch (error) {
            console.error('Error creating sequence:', error);
            throw error;
        }
    }

    // Send email with rotation
    async sendSequenceEmailWithRotation(prospectId, templateKey, sequenceId) {
        try {
            // Obtener datos del prospect
            const prospectResult = await this.pool.query(
                'SELECT * FROM leads WHERE id = $1',
                [prospectId]
            );
            
            if (prospectResult.rows.length === 0) {
                throw new Error('Prospect not found');
            }
            
            const prospect = prospectResult.rows[0];
            
            // Obtener datos de la secuencia
            const sequenceResult = await this.pool.query(
                'SELECT * FROM sequences WHERE id = $1',
                [sequenceId]
            );
            
            if (sequenceResult.rows.length === 0) {
                throw new Error('Sequence not found');
            }
            
            const sequence = sequenceResult.rows[0];
            
            // Si no hay transporters cargados, crearlos
            if (!this.transporters.has(sequenceId)) {
                const senderEmails = sequence.sender_emails || [];
                await this.createTransportersForSequence(sequenceId, senderEmails);
            }
            
            // Obtener siguiente transporter
            const transporterObj = this.getNextTransporter(sequenceId, sequence.distribution_method);
            
            if (!transporterObj) {
                throw new Error('Daily email limit reached for all senders');
            }
            
            // Obtener template
            const template = this.emailTemplates[templateKey];
            if (!template) {
                throw new Error(`Template ${templateKey} not found`);
            }
            
            // Personalizar email
            const { subject, body } = this.personalizeEmail(template, prospect);
            
            // Crear HTML del email
            const htmlBody = this.createEmailHTML(body, transporterObj.email);
            
            // Enviar email
            const mailOptions = {
                from: `Javier Alvarez <${transporterObj.email}>`,
                to: prospect.email,
                subject: subject,
                html: htmlBody,
                text: body + '\n\nJavier Alvarez\nCo-Founder, Tribearium Solutions LLC\n(817) 371 9079 | tribeariumsolutions.com'
            };
            
            const result = await transporterObj.transporter.sendMail(mailOptions);
            
            console.log(`✅ Email sent from ${transporterObj.email} to ${prospect.email}`);
            
            if (process.env.STREAM_TRANSPORT === 'true' && result.message) {
                try {
                    const chunks = [];
                    for await (const chunk of result.message) {
                        chunks.push(Buffer.from(chunk));
                    }
                    const buffer = Buffer.concat(chunks);
                    const outputDir = path.join(__dirname, 'outbox');
                    if (!fs.existsSync(outputDir)) {
                        fs.mkdirSync(outputDir, { recursive: true });
                    }
                    const filePath = path.join(outputDir, `${result.messageId.replace(/[<>]/g, '')}.eml`);
                    fs.writeFileSync(filePath, buffer);
                    console.log(`✅ Email saved to ${filePath}`);
                } catch (e) {
                    console.error('❌ Error saving .eml file:', e);
                }
            }

            // Actualizar status del prospect
            await this.pool.query(`
                UPDATE leads 
                SET 
                    email_sequence_status = 'contacted',
                    sequence_step = $1,
                    last_email_sent = NOW(),
                    emails_sent = COALESCE(emails_sent, 0) + 1,
                    updated_at = NOW()
                WHERE id = $2
            `, [templateKey, prospectId]);
            
            // Log email con remitente
            await this.logEmailSentWithSender(prospectId, subject, body, 'sent', result.messageId, templateKey, transporterObj.email);
            
            return { 
                success: true, 
                messageId: result.messageId,
                sentFrom: transporterObj.email
            };
            
        } catch (error) {
            console.error('Error sending sequence email with rotation:', error);
            return { success: false, error: error.message };
        }
    }

    // Bulk send with rotation
    async sendBulkSequenceEmails(sequenceId, templateKey, prospectIds = null) {
        try {
            console.log(`📧 Bulk sending ${templateKey} emails for sequence ${sequenceId} with rotation`);
            
            // Obtener sequence data
            const sequenceResult = await this.pool.query(
                'SELECT * FROM sequences WHERE id = $1',
                [sequenceId]
            );
            
            if (sequenceResult.rows.length === 0) {
                throw new Error('Sequence not found');
            }
            
            const sequence = sequenceResult.rows[0];
            
            // Cargar transporters si no están cargados
            if (!this.transporters.has(sequenceId)) {
                const senderEmails = sequence.sender_emails || [];
                await this.createTransportersForSequence(sequenceId, senderEmails);
            }
            
            // Obtener prospects
            let query = `
                SELECT * FROM leads 
                WHERE sequence_id = $1 
                AND email_sequence_status = 'in_sequence'
            `;
            let params = [sequenceId];
            
            if (prospectIds && prospectIds.length > 0) {
                query += ' AND id = ANY($2)';
                params.push(prospectIds);
            }
            
            const prospects = await this.pool.query(query, params);
            
            console.log(`Found ${prospects.rows.length} prospects for bulk sending`);
            console.log(`Using ${this.transporters.get(sequenceId)?.length || 1} sender email(s)`);
            
            let sent = 0;
            let failed = 0;
            const results = [];
            const senderStats = new Map();
            
            for (const prospect of prospects.rows) {
                try {
                    const result = await this.sendSequenceEmailWithRotation(prospect.id, templateKey, sequenceId);
                    
                    if (result.success) {
                        sent++;
                        
                        // Track stats per sender
                        const currentCount = senderStats.get(result.sentFrom) || 0;
                        senderStats.set(result.sentFrom, currentCount + 1);
                        
                        results.push({ 
                            prospectId: prospect.id, 
                            email: prospect.email,
                            sentFrom: result.sentFrom,
                            status: 'sent',
                            messageId: result.messageId 
                        });
                    } else {
                        failed++;
                        results.push({ 
                            prospectId: prospect.id, 
                            email: prospect.email, 
                            status: 'failed',
                            error: result.error 
                        });
                    }
                    
                    // Delay between emails
                    await this.delay(2000);
                    
                } catch (error) {
                    failed++;
                    results.push({ 
                        prospectId: prospect.id, 
                        email: prospect.email, 
                        status: 'failed',
                        error: error.message 
                    });
                }
            }
            
            // Log distribution stats
            console.log('📊 Email distribution:');
            senderStats.forEach((count, email) => {
                console.log(`   ${email}: ${count} emails sent`);
            });
            
            console.log(`✅ Bulk send completed: ${sent} sent, ${failed} failed`);
            
            return {
                success: true,
                sent: sent,
                failed: failed,
                senderDistribution: Object.fromEntries(senderStats),
                results: results
            };
            
        } catch (error) {
            console.error('Error in bulk send with rotation:', error);
            return { success: false, error: error.message };
        }
    }

    // Get sender statistics
    async getSenderStatistics(sequenceId) {
        try {
            const stats = await this.pool.query(`
                SELECT 
                    sent_from as sender_email,
                    COUNT(*) as total_sent,
                    COUNT(CASE WHEN status = 'sent' THEN 1 END) as successful,
                    COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed,
                    MAX(sent_at) as last_sent_at
                FROM email_logs
                WHERE template_used IN (
                    SELECT unnest(templates::text[]) 
                    FROM sequences 
                    WHERE id = $1
                )
                GROUP BY sent_from
                ORDER BY total_sent DESC
            `, [sequenceId]);
            
            return stats.rows;
            
        } catch (error) {
            console.error('Error getting sender statistics:', error);
            return [];
        }
    }

    // =====================
    // HELPER FUNCTIONS
    // =====================
    
    createEmailHTML(body, senderEmail = null) {
        const senderInfo = senderEmail ? `Sent from: ${senderEmail}` : '';
        
        return `
            <div style="font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px;">
                ${body.replace(/\n/g, '<br>').replace(/https:\/\/[^\s<]+/g, match => `<a href="${match}" style="color: #0066cc;">${match}</a>`)}
                
                <br><br>
                <div style="border-top: 1px solid #eee; padding-top: 20px; margin-top: 30px;">
                    <table style="border-collapse: collapse;">
                        <tr>
                            <td style="padding-right: 15px; vertical-align: top;">
                                <img src="https://framerusercontent.com/images/vD3JnPphLTcWcxDoQ4c9UB15NVA.jpeg?width=100&height=100" 
                                     alt="Javier Alvarez" 
                                     style="width: 60px; height: 60px; border-radius: 50%; object-fit: cover;" />
                            </td>
                            <td style="vertical-align: top;">
                                <div style="font-weight: bold; font-size: 16px; color: #2c3e50;">Javier Alvarez</div>
                                <div style="color: #7f8c8d; font-size: 14px;">Co-Founder, Tribearium Solutions LLC</div>
                                <div style="color: #7f8c8d; font-size: 12px; margin-top: 5px;">
                                    📞 (817) 371 9079 | 🌐 <a href="https://tribeariumsolutions.com" style="color: #7f8c8d; text-decoration: none;">tribeariumsolutions.com</a>
                                </div>
                            </td>
                        </tr>
                    </table>
                </div>
            </div>
        `;
    }
    
    async logEmailSentWithSender(leadId, subject, body, status, messageId, templateKey, senderEmail) {
        try {
            await this.pool.query(`
                INSERT INTO email_logs (
                    lead_id, email_address, subject, body, status, 
                    tracking_id, template_used, sent_from, sent_at
                )
                SELECT l.id, l.email, $2, $3, $4, $5, $6, $7, NOW()
                FROM leads l WHERE l.id = $1
            `, [leadId, subject, body, status, messageId, templateKey, senderEmail]);
        } catch (error) {
            console.error('Error logging email:', error);
        }
    }

    // =====================
    // EXISTING METHODS (mantener compatibilidad)
    // =====================
    
    processSpinSyntax(text) {
        return text.replace(/\{spin\}([^{]+)\{endspin\}/g, (match, options) => {
            const choices = options.split('|');
            const randomIndex = Math.floor(Math.random() * choices.length);
            return choices[randomIndex];
        });
    }

    personalizeEmail(template, prospectData) {
        let subject = template.subject;
        let body = template.body;
        
        const replacements = {
            '{{name}}': prospectData.name ? prospectData.name.split(' ')[0] : 'there',
            '{{company}}': prospectData.company || 'your company',
            '{{industry}}': prospectData.industry || 'your industry',
            '{{title}}': prospectData.title || ''
        };
        
        for (const [placeholder, value] of Object.entries(replacements)) {
            const regex = new RegExp(placeholder, 'g');
            subject = subject.replace(regex, value);
            body = body.replace(regex, value);
        }
        
        subject = this.processSpinSyntax(subject);
        body = this.processSpinSyntax(body);
        
        return { subject, body };
    }

    async sendSequenceEmail(prospectId, templateKey, sequenceId) {
        // Redirect to rotation method
        return this.sendSequenceEmailWithRotation(prospectId, templateKey, sequenceId);
    }

    async addProspectsToSequence(sequenceId, prospectIds) {
        try {
            console.log(`Adding ${prospectIds.length} prospects to sequence ${sequenceId}`);
            
            const result = await this.pool.query(`
                UPDATE leads 
                SET 
                    email_sequence_status = 'in_sequence',
                    sequence_id = $1,
                    sequence_step = 'day_1',
                    sequence_added_at = NOW(),
                    updated_at = NOW()
                WHERE id = ANY($2)
                RETURNING id, name, email, company
            `, [sequenceId, prospectIds]);
            
            await this.pool.query(`
                UPDATE sequences 
                SET prospects_count = prospects_count + $1
                WHERE id = $2
            `, [result.rows.length, sequenceId]);
            
            return {
                success: true,
                added: result.rows.length,
                prospects: result.rows
            };
            
        } catch (error) {
            console.error('Error adding prospects to sequence:', error);
            throw error;
        }
    }

    async getSequences() {
        try {
            const result = await this.pool.query(`
                SELECT 
                    s.*,
                    COUNT(l.id) as prospects_count,
                    COUNT(CASE WHEN l.email_sequence_status = 'contacted' THEN 1 END) as contacted,
                    COUNT(CASE WHEN l.email_opened = true THEN 1 END) as opened,
                    COUNT(CASE WHEN l.email_replied = true THEN 1 END) as replied,
                    COALESCE(s.sender_emails, '[]'::jsonb) as sender_emails,
                    s.distribution_method,
                    s.daily_limit_per_email
                FROM sequences s
                LEFT JOIN leads l ON s.id = l.sequence_id
                GROUP BY s.id
                ORDER BY s.created_at DESC
            `);
            
            return result.rows.map(row => ({
                id: row.id,
                name: row.name,
                description: row.description,
                templates: typeof row.templates === 'string' ? JSON.parse(row.templates) : row.templates,
                status: row.status,
                created_by: row.created_by,
                prospects_count: parseInt(row.prospects_count),
                contacted: parseInt(row.contacted),
                opened: parseInt(row.opened),
                replied: parseInt(row.replied),
                sender_emails_count: row.sender_emails ? row.sender_emails.length : 1,
                distribution_method: row.distribution_method || 'round-robin',
                daily_limit_per_email: row.daily_limit_per_email || 50,
                created_at: row.created_at,
                updated_at: row.updated_at
            }));
            
        } catch (error) {
            console.error('Error getting sequences:', error);
            return [];
        }
    }

    async getSequenceAnalytics(sequenceId) {
        try {
            const sequenceData = await this.pool.query(`
                SELECT 
                    s.*,
                    COUNT(l.id) as total_prospects,
                    COUNT(CASE WHEN l.emails_sent > 0 THEN 1 END) as contacted,
                    COUNT(CASE WHEN l.email_opened = true THEN 1 END) as opened,
                    COUNT(CASE WHEN l.email_replied = true THEN 1 END) as replied
                FROM sequences s
                LEFT JOIN leads l ON s.id = l.sequence_id
                WHERE s.id = $1
                GROUP BY s.id
            `, [sequenceId]);
            
            if (sequenceData.rows.length === 0) {
                return null;
            }
            
            const data = sequenceData.rows[0];
            
            // Get sender statistics
            const senderStats = await this.getSenderStatistics(sequenceId);
            
            return {
                sequence: {
                    id: data.id,
                    name: data.name,
                    status: data.status,
                    sender_emails_count: data.sender_emails ? data.sender_emails.length : 1,
                    distribution_method: data.distribution_method,
                    created_at: data.created_at
                },
                stats: {
                    total_prospects: parseInt(data.total_prospects),
                    contacted: parseInt(data.contacted),
                    opened: parseInt(data.opened),
                    replied: parseInt(data.replied),
                    open_rate: Math.round((parseInt(data.opened) / Math.max(parseInt(data.contacted), 1)) * 100),
                    reply_rate: Math.round((parseInt(data.replied) / Math.max(parseInt(data.contacted), 1)) * 100)
                },
                senderStats: senderStats
            };
            
        } catch (error) {
            console.error('Error getting sequence analytics:', error);
            return null;
        }
    }

    async pauseSequenceForProspect(prospectId) {
        try {
            await this.pool.query(`
                UPDATE leads 
                SET email_sequence_status = 'paused', updated_at = NOW()
                WHERE id = $1
            `, [prospectId]);
            
            return { success: true };
        } catch (error) {
            console.error('Error pausing sequence:', error);
            return { success: false, error: error.message };
        }
    }

    async resumeSequenceForProspect(prospectId) {
        try {
            await this.pool.query(`
                UPDATE leads 
                SET email_sequence_status = 'in_sequence', updated_at = NOW()
                WHERE id = $1
            `, [prospectId]);
            
            return { success: true };
        } catch (error) {
            console.error('Error resuming sequence:', error);
            return { success: false, error: error.message };
        }
    }

    async markProspectAsReplied(prospectId, sequenceId = null) {
        try {
            await this.pool.query(`
                UPDATE leads 
                SET 
                    email_sequence_status = 'replied',
                    email_replied = true,
                    replied_at = NOW(),
                    updated_at = NOW()
                WHERE id = $1
            `, [prospectId]);
            
            if (sequenceId) {
                await this.pool.query(`
                    UPDATE sequences 
                    SET replied = COALESCE(replied, 0) + 1
                    WHERE id = $1
                `, [sequenceId]);
            }
            
            return { success: true };
        } catch (error) {
            console.error('Error marking as replied:', error);
            return { success: false, error: error.message };
        }
    }

    async removeProspectFromSequence(prospectId, sequenceId) {
        try {
            await this.pool.query(`
                UPDATE leads 
                SET 
                    email_sequence_status = 'removed',
                    sequence_id = NULL,
                    sequence_step = NULL,
                    updated_at = NOW()
                WHERE id = $1
            `, [prospectId]);
            
            await this.pool.query(`
                UPDATE sequences 
                SET prospects_count = GREATEST(prospects_count - 1, 0)
                WHERE id = $1
            `, [sequenceId]);
            
            return { success: true };
        } catch (error) {
            console.error('Error removing from sequence:', error);
            return { success: false, error: error.message };
        }
    }

    async getOverallStats() {
        try {
            const stats = await this.pool.query(`
                SELECT 
                    COUNT(DISTINCT sequence_id) as total_sequences,
                    COUNT(*) as total_prospects,
                    COUNT(CASE WHEN emails_sent > 0 THEN 1 END) as contacted,
                    COUNT(CASE WHEN email_opened = true THEN 1 END) as opened,
                    COUNT(CASE WHEN email_replied = true THEN 1 END) as replied,
                    SUM(COALESCE(emails_sent, 0)) as total_emails_sent
                FROM leads 
                WHERE sequence_id IS NOT NULL
            `);
            
            const data = stats.rows[0];
            const contacted = parseInt(data.contacted) || 1;
            
            return {
                total_sequences: parseInt(data.total_sequences) || 0,
                total_prospects: parseInt(data.total_prospects) || 0,
                contacted: parseInt(data.contacted) || 0,
                opened: parseInt(data.opened) || 0,
                replied: parseInt(data.replied) || 0,
                total_emails_sent: parseInt(data.total_emails_sent) || 0,
                open_rate: Math.round((parseInt(data.opened) / contacted) * 100),
                reply_rate: Math.round((parseInt(data.replied) / contacted) * 100)
            };
            
        } catch (error) {
            console.error('Error getting overall stats:', error);
            return {
                total_sequences: 0, total_prospects: 0, contacted: 0,
                opened: 0, replied: 0, total_emails_sent: 0,
                open_rate: 0, reply_rate: 0
            };
        }
    }

    async logEmailSent(leadId, subject, body, status, messageId, templateKey = null, errorMessage = null) {
        return this.logEmailSentWithSender(leadId, subject, body, status, messageId, templateKey, process.env.GMAIL_USER);
    }

    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    async initializeTables() {
        try {
            // Create sequences table with rotation columns
            await this.pool.query(`
                CREATE TABLE IF NOT EXISTS sequences (
                    id VARCHAR(50) PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    description TEXT,
                    templates JSONB DEFAULT '[]',
                    status VARCHAR(50) DEFAULT 'draft',
                    created_by VARCHAR(255),
                    prospects_count INTEGER DEFAULT 0,
                    contacted INTEGER DEFAULT 0,
                    opened INTEGER DEFAULT 0,
                    replied INTEGER DEFAULT 0,
                    positive INTEGER DEFAULT 0,
                    sender_emails JSONB DEFAULT '[]',
                    distribution_method VARCHAR(50) DEFAULT 'round-robin',
                    daily_limit_per_email INTEGER DEFAULT 50,
                    email_rotation_index INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            `);
            
            // Update email_logs table to include sent_from
            await this.pool.query(`
                ALTER TABLE email_logs 
                ADD COLUMN IF NOT EXISTS sent_from VARCHAR(255)
            `);
            
            // Add sequence columns to leads table
            await this.pool.query(`
                ALTER TABLE leads 
                ADD COLUMN IF NOT EXISTS sequence_id VARCHAR(50),
                ADD COLUMN IF NOT EXISTS sequence_step VARCHAR(20),
                ADD COLUMN IF NOT EXISTS sequence_added_at TIMESTAMP,
                ADD COLUMN IF NOT EXISTS emails_sent INTEGER DEFAULT 0,
                ADD COLUMN IF NOT EXISTS email_opened BOOLEAN DEFAULT false,
                ADD COLUMN IF NOT EXISTS email_replied BOOLEAN DEFAULT false,
                ADD COLUMN IF NOT EXISTS email_positive BOOLEAN DEFAULT false,
                ADD COLUMN IF NOT EXISTS last_email_sent TIMESTAMP,
                ADD COLUMN IF NOT EXISTS replied_at TIMESTAMP
            `);
            
            console.log('✅ Database tables initialized with email rotation support');
            
        } catch (error) {
            console.error('Error initializing tables:', error);
        }
    }
}

// Create API routes
function createSalesHandyEmailRoutes(app, emailSystem) {
    
    app.post('/api/sequences', async (req, res) => {
        try {
            const sequence = await emailSystem.createSequence(req.body);
            res.json({ success: true, data: sequence });
        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    });
    
    app.get('/api/sequences', async (req, res) => {
        try {
            const sequences = await emailSystem.getSequences();
            res.json({ success: true, data: sequences });
        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    });
    
    app.post('/api/sequences/:id/prospects', async (req, res) => {
        try {
            const result = await emailSystem.addProspectsToSequence(req.params.id, req.body.prospect_ids);
            res.json({ success: true, data: result });
        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    });
    
    app.post('/api/sequences/:sequenceId/send-email/:prospectId', async (req, res) => {
        try {
            const { templateKey } = req.body;
            const result = await emailSystem.sendSequenceEmail(req.params.prospectId, templateKey, req.params.sequenceId);
            res.json({ success: result.success, data: result });
        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    });
    
    app.post('/api/sequences/:sequenceId/send-bulk', async (req, res) => {
        try {
            const { templateKey, prospect_ids } = req.body;
            const result = await emailSystem.sendBulkSequenceEmails(req.params.sequenceId, templateKey, prospect_ids);
            res.json({ success: result.success, data: result });
        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    });
    
    app.get('/api/sequences/:id/analytics', async (req, res) => {
        try {
            const analytics = await emailSystem.getSequenceAnalytics(req.params.id);
            res.json({ success: true, data: analytics });
        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    });
    
    app.get('/api/sequences/:id/sender-stats', async (req, res) => {
        try {
            const stats = await emailSystem.getSenderStatistics(req.params.id);
            res.json({ success: true, data: stats });
        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    });
    
    app.get('/api/email-automation/stats', async (req, res) => {
        try {
            const stats = await emailSystem.getOverallStats();
            res.json({ success: true, data: stats });
        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    });
    
    app.post('/api/prospects/:id/pause-sequence', async (req, res) => {
        try {
            const result = await emailSystem.pauseSequenceForProspect(req.params.id);
            res.json(result);
        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    });
    
    app.post('/api/prospects/:id/resume-sequence', async (req, res) => {
        try {
            const result = await emailSystem.resumeSequenceForProspect(req.params.id);
            res.json(result);
        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    });
    
    app.post('/api/prospects/:id/mark-replied', async (req, res) => {
        try {
            const { sequenceId } = req.body;
            const result = await emailSystem.markProspectAsReplied(req.params.id, sequenceId);
            res.json(result);
        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    });
    
    app.delete('/api/prospects/:id/remove-from-sequence', async (req, res) => {
        try {
            const { sequenceId } = req.body;
            const result = await emailSystem.removeProspectFromSequence(req.params.id, sequenceId);
            res.json(result);
        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    });
}

module.exports = { SalesHandyEmailSystem, createSalesHandyEmailRoutes };

// CLI execution for testing
if (require.main === module) {
    const emailSystem = new SalesHandyEmailSystem();
    
    console.log('🧪 Testing Email System with Rotation...');
    
    emailSystem.initializeTables()
        .then(() => {
            console.log('✅ Database initialized with rotation support');
            console.log('🎉 === EMAIL ROTATION SYSTEM READY ===');
            console.log('');
            console.log('🔧 NUEVAS CARACTERÍSTICAS:');
            console.log('  ✅ Rotación de múltiples cuentas Gmail');
            console.log('  ✅ Distribución round-robin, random o weighted');
            console.log('  ✅ Límites diarios por cuenta');
            console.log('  ✅ Estadísticas por remitente');
            console.log('  ✅ Gestión automática de transporters');
            
            process.exit(0);
        })
        .catch(error => {
            console.error('❌ System test failed:', error);
            process.exit(1);
        });
}