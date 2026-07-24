/**
 * scan-qr-codes.js
 * ------------------------------------------------------------
 * TAHAP 2 dari pipeline QR-scraper.
 *
 * Input : video_sources.csv (hasil extract-video-sources.js)
 *         kolom: session_url,title,video_src,video_type
 *
 * Proses untuk tiap video:
 *   1. Panggil `ffmpeg` untuk mengambil frame setiap N detik (FPS_SAMPLE)
 *      langsung dari video_src (mp4/m3u8) -> tidak perlu "menonton" video,
 *      ffmpeg cukup mem-parsing stream-nya.
 *   2. Panggil `zbarimg` (dari paket zbar-tools) untuk membaca QR code di
 *      tiap frame.
 *   3. Kalau ada QR code ke-detect & isinya berupa URL, dicatat ke CSV.
 *   4. Frame sementara dihapus supaya tidak memenuhi disk.
 *
 * Output: qr_codes.csv
 *         kolom: session_url,title,qr_url,approx_time_seconds,frame_file
 *
 * PREREQUISITE (install dulu di sistem, BUKAN via npm):
 *   Ubuntu/Debian : sudo apt-get install -y ffmpeg zbar-tools
 *   macOS (brew)  : brew install ffmpeg zbar
 *
 * CARA PAKAI:
 *   node scan-qr-codes.js
 *
 * KONFIGURASI (env var, semua opsional):
 *   FPS_SAMPLE=1        -> ambil 1 frame per detik (default 0.5 = 1 frame/2 detik)
 *   SCAN_START=0         -> mulai scan dari detik ke berapa (default: dari awal)
 *   SCAN_DURATION=        -> batasi durasi discan dalam detik (default: full video)
 *                            contoh: SCAN_DURATION=180 kalau QR biasanya cuma
 *                            muncul di 3 menit pertama/terakhir
 *   CONCURRENCY=1         -> jumlah video diproses paralel (hati-hati, berat di CPU/RAM)
 *   KEEP_FRAMES=false     -> set true kalau mau simpan semua frame (untuk debug)
 * ------------------------------------------------------------
 */

const fs = require('fs');
const path = require('path');
const os = require('os');
const { spawn, execSync } = require('child_process');

// ===================== CONFIG =====================
const INPUT_CSV = process.env.INPUT_CSV || path.join(__dirname, 'video_sources.csv');
const OUTPUT_CSV = process.env.OUTPUT_CSV || path.join(__dirname, 'qr_codes.csv');
const PROCESSED_LOG = process.env.PROCESSED_LOG || path.join(__dirname, 'qr_scan_processed.log');

const FPS_SAMPLE = parseFloat(process.env.FPS_SAMPLE || '0.5'); // 0.5 = 1 frame tiap 2 detik
const SCAN_START = process.env.SCAN_START ? parseFloat(process.env.SCAN_START) : null;
const SCAN_DURATION = process.env.SCAN_DURATION ? parseFloat(process.env.SCAN_DURATION) : null;
const KEEP_FRAMES = process.env.KEEP_FRAMES === 'true';
const TMP_ROOT = process.env.TMP_ROOT || path.join(os.tmpdir(), 'nvidia-qr-frames');
// ====================================================

function checkBinary(name) {
  try {
    execSync(`${name} -version`, { stdio: 'ignore' });
    return true;
  } catch (e) {
    try {
      execSync(`${name} --version`, { stdio: 'ignore' });
      return true;
    } catch (e2) {
      return false;
    }
  }
}

function parseCsv(content) {
  const lines = content.split(/\r?\n/).filter((l) => l.length > 0);
  const header = splitCsvLine(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const fields = splitCsvLine(lines[i]);
    if (fields.length !== header.length) continue;
    const row = {};
    header.forEach((h, idx) => (row[h] = fields[idx]));
    rows.push(row);
  }
  return rows;
}

function splitCsvLine(line) {
  const result = [];
  let cur = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (inQuotes) {
      if (c === '"') {
        if (line[i + 1] === '"') {
          cur += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        cur += c;
      }
    } else {
      if (c === '"') inQuotes = true;
      else if (c === ',') {
        result.push(cur);
        cur = '';
      } else cur += c;
    }
  }
  result.push(cur);
  return result;
}

function csvEscape(value) {
  if (value === null || value === undefined) return '';
  return `"${String(value).replace(/"/g, '""')}"`;
}

function ensureCsvWithHeader(filePath, header) {
  if (!fs.existsSync(filePath)) fs.writeFileSync(filePath, header + '\n', 'utf8');
}

function appendCsvRow(filePath, fields) {
  fs.appendFileSync(filePath, fields.map(csvEscape).join(',') + '\n', 'utf8');
}

function loadProcessedSet() {
  if (!fs.existsSync(PROCESSED_LOG)) return new Set();
  return new Set(fs.readFileSync(PROCESSED_LOG, 'utf8').split(/\r?\n/).filter(Boolean));
}

function markProcessed(sessionUrl) {
  fs.appendFileSync(PROCESSED_LOG, sessionUrl + '\n', 'utf8');
}

function runCmd(cmd, args) {
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args, { stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    p.stdout.on('data', (d) => (stdout += d.toString()));
    p.stderr.on('data', (d) => (stderr += d.toString()));
    p.on('close', (code) => {
      resolve({ code, stdout, stderr });
    });
    p.on('error', reject);
  });
}

async function extractFrames(videoSrc, outDir) {
  fs.mkdirSync(outDir, { recursive: true });

  const args = ['-y', '-hide_banner', '-loglevel', 'error'];
  if (SCAN_START !== null) args.push('-ss', String(SCAN_START));
  args.push('-i', videoSrc);
  if (SCAN_DURATION !== null) args.push('-t', String(SCAN_DURATION));
  args.push('-vf', `fps=${FPS_SAMPLE}`, path.join(outDir, 'frame_%06d.png'));

  const { code, stderr } = await runCmd('ffmpeg', args);
  if (code !== 0) {
    throw new Error(`ffmpeg gagal (exit ${code}): ${stderr.slice(0, 500)}`);
  }
}

async function scanFramesForQr(outDir) {
  const files = fs
    .readdirSync(outDir)
    .filter((f) => f.endsWith('.png'))
    .sort();

  const found = []; // { frame_file, frame_index, value }

  for (const file of files) {
    const framePath = path.join(outDir, file);
    // --raw : hanya print isi barcode (tanpa prefix "QR-Code:")
    // -q    : quiet, tidak print info tambahan
    const { stdout } = await runCmd('zbarimg', ['--raw', '-q', framePath]);
    const value = stdout.trim();
    if (value) {
      const match = file.match(/frame_(\d+)\.png/);
      const frameIndex = match ? parseInt(match[1], 10) : 0;
      found.push({ frame_file: file, frame_index: frameIndex, value });
    }
  }

  return found;
}

function frameIndexToSeconds(frameIndex) {
  // frame_000001 = frame pertama yang diambil ffmpeg
  const start = SCAN_START || 0;
  return start + (frameIndex - 1) / FPS_SAMPLE;
}

async function processVideo(row, index, total) {
  const { session_url: sessionUrl, title, video_src: videoSrc } = row;

  console.log(`\n[${index + 1}/${total}] ${sessionUrl}`);

  if (!videoSrc) {
    console.log('  -> Lewati, video_src kosong.');
    return;
  }

  const outDir = path.join(TMP_ROOT, `vid_${index}`);

  try {
    console.log('  -> Mengambil frame dengan ffmpeg...');
    await extractFrames(videoSrc, outDir);

    console.log('  -> Scanning QR code dengan zbarimg...');
    const found = await scanFramesForQr(outDir);

    const seenValues = new Set();
    let qrCount = 0;
    for (const item of found) {
      if (seenValues.has(item.value)) continue; // dedupe per-video
      seenValues.add(item.value);

      // Hanya simpan yang terlihat seperti URL, sesuai permintaan
      // (kalau mau simpan semua jenis QR, hapus filter ini)
      const isUrl = /^https?:\/\//i.test(item.value);
      if (!isUrl) continue;

      const approxTime = frameIndexToSeconds(item.frame_index).toFixed(1);
      appendCsvRow(OUTPUT_CSV, [sessionUrl, title, item.value, approxTime, item.frame_file]);
      qrCount++;
    }

    console.log(`  -> Selesai. ${qrCount} QR (URL) unik ditemukan.`);
  } finally {
    if (!KEEP_FRAMES && fs.existsSync(outDir)) {
      fs.rmSync(outDir, { recursive: true, force: true });
    }
    markProcessed(sessionUrl);
  }
}

async function main() {
  if (!checkBinary('ffmpeg')) {
    console.error('ffmpeg tidak ditemukan. Install dulu: sudo apt-get install ffmpeg (atau brew install ffmpeg)');
    process.exit(1);
  }
  if (!checkBinary('zbarimg')) {
    console.error('zbarimg tidak ditemukan. Install dulu: sudo apt-get install zbar-tools (atau brew install zbar)');
    process.exit(1);
  }

  if (!fs.existsSync(INPUT_CSV)) {
    console.error(`File input tidak ditemukan: ${INPUT_CSV}`);
    process.exit(1);
  }

  const rows = parseCsv(fs.readFileSync(INPUT_CSV, 'utf8'));
  console.log(`Ditemukan ${rows.length} video di ${INPUT_CSV}`);
  console.log(`Sampling ${FPS_SAMPLE} fps (1 frame tiap ${(1 / FPS_SAMPLE).toFixed(1)} detik)`);

  ensureCsvWithHeader(OUTPUT_CSV, 'session_url,title,qr_url,approx_time_seconds,frame_file');
  fs.mkdirSync(TMP_ROOT, { recursive: true });

  const processed = loadProcessedSet();

  for (let i = 0; i < rows.length; i++) {
    if (processed.has(rows[i].session_url)) {
      console.log(`[${i + 1}/${rows.length}] Sudah pernah diproses, lewati: ${rows[i].session_url}`);
      continue;
    }
    try {
      await processVideo(rows[i], i, rows.length);
    } catch (err) {
      console.error(`  !! Error memproses video: ${err.message}`);
    }
  }

  console.log(`\nSelesai semua. Hasil QR code: ${OUTPUT_CSV}`);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
