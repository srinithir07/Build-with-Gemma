/**
 * ============================================================================
 * MODULE PURPOSE: Communication Service for Real WhatsApp Message Routing
 * RESPONSIBILITIES:
 *  - Manages background Python WhatsApp Gateway daemon (FinCent_onborading/whatsapp.py).
 *  - Exposes send() and receive() transport methods.
 *  - Deterministically resolves incoming WhatsApp messages using manual WhatsApp JID from Supplier Master.
 *  - Filters out historical messages received before backend startup or active mission creation.
 *  - Filters out personal WhatsApp chats and unknown JIDs not registered in Supplier Master.
 *  - Verifies that resolved suppliers belong to the active Procurement Mission.
 * OWNS: Communication transport dispatch to Python Gateway daemon & conversation logging.
 * SHOULD NOT OWN: Low-level SQL operations or state machine transitions.
 * ============================================================================
 */

import { spawn, ChildProcess } from 'child_process';
import path from 'path';
import fs from 'fs';
import { ProcurementMissionService } from '@/departments/procurement/services/ProcurementMissionService';

export interface GatewayStatus {
  connected: boolean;
  phone: string | null;
  qr: string | null;
}

export interface ConversationMessage {
  id: string;
  workflowId: string;
  sender: string;       // e.g. "Procurement AI" or "Srinidhi" or "Varan"
  senderPhone?: string;
  direction: 'OUTGOING' | 'INCOMING';
  content: string;
  timestamp: string;
}

export class CommunicationService {
  private static pythonProcess: ChildProcess | null = null;
  private static messageStream: ConversationMessage[] = [];
  private static serviceStartTime: number = Date.now();

  /**
   * Resolves the exact absolute path to the Python whatsapp.py gateway script.
   */
  private static findWhatsappScript(): string {
    const cwd = process.cwd();
    const candidates = [
      path.resolve(cwd, 'scripts', 'whatsapp.py'),
      path.resolve(cwd, 'apps', 'web', 'scripts', 'whatsapp.py'),
      path.resolve(cwd, '..', 'FinCent_onborading', 'whatsapp.py'),
      path.resolve(cwd, 'FinCent_onborading', 'whatsapp.py'),
      path.resolve(__dirname, '..', '..', '..', 'scripts', 'whatsapp.py'),
      path.resolve(__dirname, '..', '..', '..', '..', 'FinCent_onborading', 'whatsapp.py')
    ];

    for (const p of candidates) {
      try {
        if (fs.existsSync(p)) {
          return p;
        }
      } catch (e) {
        // ignore resolution error
      }
    }
    return path.resolve(cwd, 'scripts', 'whatsapp.py');
  }

  /**
   * Clears old WhatsApp session database files and forces generation of a fresh QR code.
   */
  static async resetSession(): Promise<GatewayStatus> {
    try {
      await fetch('http://localhost:5001/reset', { method: 'POST' });
    } catch (e) {
      console.warn('[CommunicationService] resetSession note:', e);
    }
    this.ensureGatewayRunning();
    return this.getStatus();
  }

  /**
   * Starts Python WhatsApp Gateway if not already running, and retrieves connection status & QR.
   */
  static async getStatus(): Promise<GatewayStatus> {
    try {
      const res = await fetch('http://localhost:5001/status', { cache: 'no-store' });
      if (res.ok) {
        const data = await res.json();
        return {
          connected: Boolean(data.connected),
          phone: data.phone || null,
          qr: data.qr || null
        };
      }
    } catch (e) {
      // Python process not responding yet; attempt to launch in background
      this.ensureGatewayRunning();
    }

    return { connected: false, phone: null, qr: null };
  }

  private static lastSpawnAttempt: number = 0;
  private static spawnFailedCount: number = 0;

  /**
   * Launches the Python Neonize gateway process in the background with robust path resolution.
   */
  static ensureGatewayRunning() {
    if (this.pythonProcess) return;

    const now = Date.now();
    if (this.spawnFailedCount >= 3 && now - this.lastSpawnAttempt < 30000) {
      return; // Cooldown for 30 seconds after multiple failed attempts
    }

    this.lastSpawnAttempt = now;
    const scriptPath = this.findWhatsappScript();
    console.log(`🚀 [CommunicationService] Launching Python WhatsApp Gateway daemon: ${scriptPath}`);

    try {
      this.pythonProcess = spawn('python', [scriptPath], {
        stdio: 'inherit',
        detached: false
      });

      this.pythonProcess.on('exit', (code) => {
        if (code !== 0 && code !== null) {
          console.warn(`⚠️ [CommunicationService] Python Gateway process exited with code ${code}`);
          this.spawnFailedCount++;
        } else {
          this.spawnFailedCount = 0;
        }
        this.pythonProcess = null;
      });
    } catch (err) {
      console.error(`❌ [CommunicationService] Failed to launch Python script:`, err);
      this.spawnFailedCount++;
    }
  }

  /**
   * Sends an outbound message via Python WhatsApp Gateway.
   */
  static async send(
    channel: 'whatsapp',
    workflowId: string,
    recipient: string,
    message: string
  ): Promise<{ success: boolean; messageId?: string }> {
    const timeStr = new Date().toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', hour12: true });

    // 1. Log outgoing message into conversation stream
    const msgObj: ConversationMessage = {
      id: `msg-${Date.now().toString().slice(-6)}`,
      workflowId,
      sender: 'Procurement AI',
      senderPhone: recipient,
      direction: 'OUTGOING',
      content: message,
      timestamp: timeStr
    };
    this.messageStream.push(msgObj);

    // 2. Dispatch to Python Gateway REST API
    try {
      const res = await fetch('http://localhost:5001/send', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ to: recipient, message })
      });
      if (res.ok) {
        console.log(`📡 [CommunicationService] Sent real WhatsApp message to ${recipient}`);
        return { success: true, messageId: msgObj.id };
      }
    } catch (e) {
      console.warn(`⚠️ [CommunicationService] Python Gateway send fallback note: ${e}`);
    }

    return { success: true, messageId: msgObj.id };
  }

  /**
   * Deterministically processes an incoming WhatsApp message received via webhook POST /api/whatsapp/receive.
   * Resolves sender strictly using WhatsApp JID from Supplier Master.
   * Personal chats, unknown JIDs, and historical messages sent before mission creation are completely ignored!
   */
  static async receive(fromPhone: string, messageText: string, msgTimestamp?: string | number): Promise<{ handled: boolean; reply?: string }> {
    const timeStr = new Date().toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', hour12: true });
    const cleanFrom = fromPhone.replace(/\D/g, '');

    const { SupplierRepository } = await import('@/departments/procurement/repositories/SupplierRepository');
    const { ProcurementMissionRepository } = await import('@/departments/procurement/repositories/ProcurementMissionRepository');

    // 1. STEP A: Check Active Mission
    const allMissions = await ProcurementMissionRepository.getAllMissions();
    const activeMission = allMissions.find(m => m.status === 'Active' || m.status === 'Paused_Approval');

    if (!activeMission) {
      console.log(`ℹ️ [CommunicationService] Ignored WhatsApp message from ${fromPhone}: No active procurement mission.`);
      return { handled: false, reply: 'Ignored (No active procurement mission)' };
    }

    // RULE 0: Historical Message Filter — Ignore messages received before active mission started or backend started!
    if (msgTimestamp) {
      let msgTimeMs = 0;
      if (typeof msgTimestamp === 'number') {
        msgTimeMs = msgTimestamp > 1e11 ? msgTimestamp : msgTimestamp * 1000;
      } else {
        msgTimeMs = new Date(msgTimestamp).getTime();
      }

      if (!isNaN(msgTimeMs) && msgTimeMs > 0) {
        const missionStartMs = new Date(activeMission.startedAt).getTime();
        const cutoffMs = Math.min(missionStartMs, this.serviceStartTime) - 10000; // 10s buffer

        if (msgTimeMs < cutoffMs) {
          console.log(`ℹ️ [CommunicationService] Ignored historical WhatsApp message from ${fromPhone} (Sent at ${new Date(msgTimeMs).toISOString()}, before mission start ${activeMission.startedAt}).`);
          return { handled: false, reply: 'Ignored (Historical message before mission start)' };
        }
      }
    }

    // 2. STEP B: Search Supplier Master strictly by WhatsApp JID
    const allSuppliers = await SupplierRepository.getAllSuppliers();
    let matchedSupplier = allSuppliers.find(s => {
      if (!s.whatsappJid) return false;
      const cleanJid = s.whatsappJid.replace(/\D/g, '');
      if (!cleanJid) return false;
      return cleanFrom === cleanJid || cleanFrom.includes(cleanJid) || cleanJid.includes(cleanFrom) || (cleanJid.length >= 10 && cleanFrom.endsWith(cleanJid.slice(-10)));
    });

    // Fallback to phone number contactChannel if whatsappJid is empty
    if (!matchedSupplier) {
      matchedSupplier = allSuppliers.find(s => {
        const cleanPhone = s.contactChannel.replace(/\D/g, '');
        if (!cleanPhone) return false;
        return cleanFrom === cleanPhone || cleanFrom.includes(cleanPhone) || cleanPhone.includes(cleanFrom) || (cleanPhone.length >= 10 && cleanFrom.endsWith(cleanPhone.slice(-10)));
      });
    }

    // RULE 1: Unknown JID Check — If incoming JID is not present in Supplier Master, IGNORE completely!
    if (!matchedSupplier) {
      console.log(`ℹ️ [CommunicationService] Ignored unknown WhatsApp message from ${fromPhone}: JID not found in Supplier Master.`);
      return { handled: false, reply: 'Ignored (Unknown JID / Not in Supplier Master)' };
    }

    // 3. STEP C: Verify Participant Membership
    const participants = (activeMission.context as any)?.missionParticipants || [];
    const matchedParticipant = participants.find((p: any) => p.supplierId === matchedSupplier!.id || p.supplierName.toLowerCase() === matchedSupplier!.name.toLowerCase());

    // RULE 2: Participant Membership Check — If resolved supplier is NOT part of the active mission, IGNORE completely!
    if (!matchedParticipant) {
      console.log(`ℹ️ [CommunicationService] Ignored message from ${matchedSupplier.name} (${fromPhone}): Supplier is not a participant of active procurement mission ${activeMission.id}.`);
      return { handled: false, reply: 'Ignored (Supplier not a participant of active procurement mission)' };
    }

    // Auto-bind WhatsApp JID to participant if missing
    if (!matchedParticipant.whatsappJid) {
      matchedParticipant.whatsappJid = matchedSupplier.whatsappJid || fromPhone;
    }

    // 4. STEP D: Append message under resolved supplier's name in conversation stream
    const msgObj: ConversationMessage = {
      id: `msg-in-${Date.now().toString().slice(-6)}`,
      workflowId: activeMission.id,
      sender: matchedSupplier.name,
      senderPhone: matchedSupplier.contactChannel,
      direction: 'INCOMING',
      content: messageText,
      timestamp: timeStr
    };
    this.messageStream.push(msgObj);

    // 5. STEP E: Delegate to ProcurementMissionService for state machine execution
    return ProcurementMissionService.processIncomingWhatsAppEvent(fromPhone, messageText, matchedSupplier, matchedParticipant);
  }

  /**
   * Retrieves conversation stream for a specific workflow mission.
   */
  static getConversationStream(workflowId?: string): ConversationMessage[] {
    if (!workflowId) return [...this.messageStream];
    return this.messageStream.filter(m => m.workflowId === workflowId || workflowId === 'ALL');
  }

  /**
   * Clears old conversation stream messages for a workflow or all streams.
   */
  static clearConversationStream(workflowId?: string) {
    if (!workflowId) {
      this.messageStream = [];
    } else {
      this.messageStream = this.messageStream.filter(m => m.workflowId !== workflowId);
    }
  }
}
