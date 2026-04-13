from __future__ import annotations

import html
import json
import shutil
import subprocess
import uuid
from pathlib import Path
from textwrap import dedent

try:
    from PIL import Image, ImageDraw, ImageFont
except Exception:  # pragma: no cover - optional runtime dependency
    Image = None
    ImageDraw = None
    ImageFont = None


BACKGROUND = (246, 243, 236)
PANEL = (255, 255, 255)
INK = (27, 38, 44)
MUTED = (92, 104, 112)
GRID = (223, 216, 204)
ACCENT = (27, 111, 97)
ACCENT_2 = (150, 71, 48)


def _scale(values: list[float], size: int) -> list[float]:
    minimum = min(values)
    maximum = max(values)
    if maximum == minimum:
        return [size / 2 for _ in values]
    return [size - ((value - minimum) / (maximum - minimum)) * size for value in values]



def line_chart_svg(title: str, labels: list[str], values: list[float], width: int = 920, height: int = 420) -> str:
    padding = 60
    chart_width = width - padding * 2
    chart_height = height - padding * 2
    scaled_y = _scale(values, chart_height)
    step_x = chart_width / max(len(values) - 1, 1)
    points = [(padding + step_x * index, padding + scaled_y[index]) for index in range(len(values))]
    polyline = " ".join(f"{x:.2f},{y:.2f}" for x, y in points)
    y_min = min(values)
    y_max = max(values)

    label_svg: list[str] = []
    for index, (x, y) in enumerate(points):
        label_svg.append(f'<circle cx="{x:.2f}" cy="{y:.2f}" r="5" fill="#1b6f61"/>')
        label_svg.append(
            f'<text x="{x:.2f}" y="{height - 18}" text-anchor="middle" fill="#4b5a63" font-size="12">'
            f"{html.escape(labels[index])}</text>"
        )
    for tick in range(5):
        y = padding + chart_height * tick / 4
        value = y_max - (y_max - y_min) * tick / 4
        label_svg.append(f'<line x1="{padding}" y1="{y:.2f}" x2="{width - padding}" y2="{y:.2f}" stroke="#dfd8cc"/>')
        label_svg.append(
            f'<text x="{padding - 10}" y="{y + 4:.2f}" text-anchor="end" fill="#4b5a63" font-size="11">'
            f"{value:.2f}</text>"
        )

    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">'
        '<rect width="100%" height="100%" fill="#f6f3ec" rx="18" ry="18"/>'
        '<rect x="18" y="18" width="884" height="384" fill="#ffffff" rx="20" ry="20" stroke="#dfd8cc"/>'
        f'<text x="{padding}" y="32" fill="#102027" font-size="22" font-weight="700">{html.escape(title)}</text>'
        f'<polyline fill="none" stroke="#1b6f61" stroke-width="3" points="{polyline}"/>'
        + "".join(label_svg)
        + "</svg>"
    )



def bar_chart_svg(title: str, labels: list[str], values: list[float], width: int = 920, height: int = 420) -> str:
    padding = 60
    chart_width = width - padding * 2
    chart_height = height - padding * 2
    max_value = max(values) if values else 1.0
    bar_width = chart_width / max(len(values), 1) * 0.7
    gap = chart_width / max(len(values), 1) * 0.3
    bars: list[str] = []
    for index, value in enumerate(values):
        x = padding + index * (bar_width + gap) + gap / 2
        bar_height = 0 if max_value == 0 else chart_height * value / max_value
        y = padding + chart_height - bar_height
        bars.append(
            f'<rect x="{x:.2f}" y="{y:.2f}" width="{bar_width:.2f}" height="{bar_height:.2f}" '
            'fill="#964730" rx="8" ry="8"/>'
        )
        bars.append(
            f'<text x="{x + bar_width / 2:.2f}" y="{height - 18}" text-anchor="middle" fill="#4b5a63" font-size="12">'
            f"{html.escape(labels[index])}</text>"
        )
        bars.append(
            f'<text x="{x + bar_width / 2:.2f}" y="{max(y - 8, 30):.2f}" text-anchor="middle" fill="#102027" font-size="11">'
            f"{value:.2f}</text>"
        )
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">'
        '<rect width="100%" height="100%" fill="#f6f3ec" rx="18" ry="18"/>'
        '<rect x="18" y="18" width="884" height="384" fill="#ffffff" rx="20" ry="20" stroke="#dfd8cc"/>'
        f'<text x="{padding}" y="32" fill="#102027" font-size="22" font-weight="700">{html.escape(title)}</text>'
        + "".join(bars)
        + "</svg>"
    )



def _load_font(size: int, bold: bool = False):
    if ImageFont is None:
        raise RuntimeError("Pillow 不可用")
    candidates = []
    if bold:
        candidates.extend(["msyhbd.ttc", "Microsoft YaHei Bold", "SimHei", "DejaVuSans-Bold.ttf", "arialbd.ttf"])
    candidates.extend(["msyh.ttc", "Microsoft YaHei", "SimHei", "DejaVuSans.ttf", "arial.ttf"])
    for candidate in candidates:
        try:
            return ImageFont.truetype(candidate, size=size)
        except OSError:
            continue
    return ImageFont.load_default()



def _text_box(draw, text: str, font) -> tuple[int, int]:
    bbox = draw.textbbox((0, 0), text, font=font)
    return bbox[2] - bbox[0], bbox[3] - bbox[1]



def _format_value(value: float) -> str:
    abs_value = abs(value)
    if abs_value >= 100000000:
        return f"{value / 100000000:.2f}亿"
    if abs_value >= 10000:
        return f"{value / 10000:.2f}万"
    return f"{value:.2f}"



def _fit_label(text: str, limit: int = 10) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"



def _draw_grid(draw, plot_left: int, plot_top: int, plot_right: int, plot_bottom: int, minimum: float, maximum: float, font) -> None:
    plot_height = plot_bottom - plot_top
    for tick in range(5):
        ratio = tick / 4
        y = plot_top + plot_height * ratio
        value = maximum - (maximum - minimum) * ratio
        draw.line((plot_left, y, plot_right, y), fill=GRID, width=1)
        draw.text((24, y - 9), _format_value(value), fill=MUTED, font=font)



def _render_with_pillow(mode: str, output_path: Path, title: str, labels: list[str], values: list[float], width: int, height: int) -> Path:
    if Image is None or ImageDraw is None or ImageFont is None:
        raise RuntimeError("Pillow 不可用")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    image = Image.new("RGB", (width, height), BACKGROUND)
    draw = ImageDraw.Draw(image)
    title_font = _load_font(28, bold=True)
    axis_font = _load_font(14)
    label_font = _load_font(15)
    value_font = _load_font(13)

    draw.rounded_rectangle((20, 20, width - 20, height - 20), radius=28, fill=PANEL, outline=GRID, width=2)
    draw.text((56, 38), title, fill=INK, font=title_font)

    plot_left = 112
    plot_top = 118
    plot_right = width - 68
    plot_bottom = height - 92
    plot_width = plot_right - plot_left
    plot_height = plot_bottom - plot_top

    minimum = min(values)
    maximum = max(values)
    if mode == "bar":
        minimum = min(0.0, minimum)
        maximum = max(0.0, maximum)
    if maximum == minimum:
        maximum += 1.0
        minimum -= 1.0

    _draw_grid(draw, plot_left, plot_top, plot_right, plot_bottom, minimum, maximum, axis_font)

    def map_y(value: float) -> float:
        return plot_top + ((maximum - value) / (maximum - minimum)) * plot_height

    if mode == "line":
        step_x = plot_width / max(len(values) - 1, 1)
        points = [(plot_left + step_x * index, map_y(values[index])) for index in range(len(values))]
        if len(points) >= 2:
            draw.line(points, fill=ACCENT, width=5)
        for index, (x, y) in enumerate(points):
            draw.ellipse((x - 6, y - 6, x + 6, y + 6), fill=ACCENT)
            label = _fit_label(labels[index], 8)
            label_width, _ = _text_box(draw, label, label_font)
            draw.text((x - label_width / 2, plot_bottom + 18), label, fill=MUTED, font=label_font)
            value_text = _format_value(values[index])
            value_width, value_height = _text_box(draw, value_text, value_font)
            draw.rounded_rectangle((x - value_width / 2 - 8, y - value_height - 20, x + value_width / 2 + 8, y - 8), radius=10, fill=(240, 247, 245))
            draw.text((x - value_width / 2, y - value_height - 16), value_text, fill=INK, font=value_font)
    else:
        slot_width = plot_width / max(len(values), 1)
        bar_width = slot_width * 0.62
        baseline_y = map_y(0.0)
        draw.line((plot_left, baseline_y, plot_right, baseline_y), fill=MUTED, width=1)
        for index, value in enumerate(values):
            x = plot_left + slot_width * index + (slot_width - bar_width) / 2
            value_y = map_y(value)
            top = min(baseline_y, value_y)
            bottom = max(baseline_y, value_y)
            draw.rounded_rectangle((x, top, x + bar_width, max(bottom, top + 1)), radius=12, fill=ACCENT_2)
            label = _fit_label(labels[index], 8)
            label_width, _ = _text_box(draw, label, label_font)
            draw.text((x + bar_width / 2 - label_width / 2, plot_bottom + 18), label, fill=MUTED, font=label_font)
            value_text = _format_value(value)
            value_width, value_height = _text_box(draw, value_text, value_font)
            draw.text((x + bar_width / 2 - value_width / 2, top - value_height - 8), value_text, fill=INK, font=value_font)

    image.save(output_path, format="JPEG", quality=92, optimize=True)
    return output_path



def _powershell_path() -> str:
    binary = shutil.which("powershell") or shutil.which("powershell.exe")
    if not binary:
        raise RuntimeError("未找到 PowerShell，且当前环境未安装 Pillow，无法生成 JPG 图表。")
    return binary



def _chart_script() -> str:
    return dedent(
        r'''
        param([string]$JsonPath)
        $ErrorActionPreference = 'Stop'
        Add-Type -AssemblyName System.Drawing

        function New-Font([float]$size, [System.Drawing.FontStyle]$style) {
            try {
                return New-Object System.Drawing.Font('Microsoft YaHei', $size, $style)
            } catch {
                return New-Object System.Drawing.Font('Segoe UI', $size, $style)
            }
        }

        function Map-Y([double]$value, [double]$minimum, [double]$maximum, [double]$top, [double]$height) {
            if ($maximum -eq $minimum) {
                return $top + ($height / 2.0)
            }
            return $top + (($maximum - $value) / ($maximum - $minimum)) * $height
        }

        $payload = Get-Content -Raw -Encoding UTF8 $JsonPath | ConvertFrom-Json
        $width = [int]$payload.width
        $height = [int]$payload.height
        $labels = @($payload.labels)
        $values = @()
        foreach ($value in $payload.values) {
            $values += [double]$value
        }
        if ($values.Count -eq 0) {
            throw 'empty values'
        }

        $bitmap = New-Object System.Drawing.Bitmap $width, $height
        $graphics = [System.Drawing.Graphics]::FromImage($bitmap)
        $graphics.SmoothingMode = [System.Drawing.Drawing2D.SmoothingMode]::AntiAlias
        $graphics.TextRenderingHint = [System.Drawing.Text.TextRenderingHint]::AntiAliasGridFit

        $background = [System.Drawing.Color]::FromArgb(246, 243, 236)
        $panel = [System.Drawing.Color]::FromArgb(255, 255, 255)
        $ink = [System.Drawing.Color]::FromArgb(31, 41, 55)
        $muted = [System.Drawing.Color]::FromArgb(91, 100, 114)
        $grid = [System.Drawing.Color]::FromArgb(224, 229, 236)
        $accent = [System.Drawing.Color]::FromArgb(21, 94, 117)
        $accentFill = [System.Drawing.Color]::FromArgb(150, 71, 48)

        $titleFont = New-Font 24 ([System.Drawing.FontStyle]::Bold)
        $labelFont = New-Font 11 ([System.Drawing.FontStyle]::Regular)
        $valueFont = New-Font 10 ([System.Drawing.FontStyle]::Regular)
        $axisBrush = New-Object System.Drawing.SolidBrush($muted)
        $inkBrush = New-Object System.Drawing.SolidBrush($ink)
        $accentBrush = New-Object System.Drawing.SolidBrush($accentFill)
        $linePen = New-Object System.Drawing.Pen($accent, 3)
        $gridPen = New-Object System.Drawing.Pen($grid, 1)
        $barPen = New-Object System.Drawing.Pen($accentFill, 1)

        try {
            $graphics.Clear($background)
            $graphics.FillRectangle((New-Object System.Drawing.SolidBrush($panel)), 24, 20, $width - 48, $height - 40)
            $graphics.DrawString([string]$payload.title, $titleFont, $inkBrush, 56, 34)

            $plotLeft = 90.0
            $plotTop = 110.0
            $plotRight = $width - 72.0
            $plotBottom = $height - 86.0
            $plotWidth = $plotRight - $plotLeft
            $plotHeight = $plotBottom - $plotTop

            $minimum = ($values | Measure-Object -Minimum).Minimum
            $maximum = ($values | Measure-Object -Maximum).Maximum

            if ([string]$payload.mode -eq 'bar') {
                $minimum = [Math]::Min(0.0, $minimum)
                $maximum = [Math]::Max(0.0, $maximum)
            }
            if ($maximum -eq $minimum) {
                $maximum += 1.0
                $minimum -= 1.0
            }

            for ($tick = 0; $tick -le 4; $tick++) {
                $ratio = $tick / 4.0
                $y = $plotTop + $plotHeight * $ratio
                $tickValue = $maximum - (($maximum - $minimum) * $ratio)
                $graphics.DrawLine($gridPen, $plotLeft, $y, $plotRight, $y)
                $graphics.DrawString(([string]::Format('{0:N2}', $tickValue)), $valueFont, $axisBrush, 14, $y - 8)
            }

            if ([string]$payload.mode -eq 'line') {
                $stepX = if ($values.Count -gt 1) { $plotWidth / ($values.Count - 1) } else { 0.0 }
                $points = New-Object 'System.Drawing.PointF[]' $values.Count
                for ($index = 0; $index -lt $values.Count; $index++) {
                    $x = $plotLeft + ($stepX * $index)
                    $y = Map-Y $values[$index] $minimum $maximum $plotTop $plotHeight
                    $points[$index] = New-Object System.Drawing.PointF([float]$x, [float]$y)
                }
                for ($index = 0; $index -lt ($points.Length - 1); $index++) {
                    $graphics.DrawLine($linePen, $points[$index], $points[$index + 1])
                }
                for ($index = 0; $index -lt $points.Length; $index++) {
                    $point = $points[$index]
                    $graphics.FillEllipse($inkBrush, $point.X - 4, $point.Y - 4, 8, 8)
                    $graphics.DrawString([string]$labels[$index], $labelFont, $axisBrush, $point.X - 20, $plotBottom + 12)
                    $graphics.DrawString(([string]::Format('{0:N2}', $values[$index])), $valueFont, $inkBrush, $point.X - 20, $point.Y - 26)
                }
            } else {
                $slotWidth = if ($values.Count -gt 0) { $plotWidth / $values.Count } else { $plotWidth }
                $barWidth = $slotWidth * 0.68
                $baselineY = Map-Y 0 $minimum $maximum $plotTop $plotHeight
                $graphics.DrawLine($gridPen, $plotLeft, $baselineY, $plotRight, $baselineY)
                for ($index = 0; $index -lt $values.Count; $index++) {
                    $x = $plotLeft + ($slotWidth * $index) + (($slotWidth - $barWidth) / 2.0)
                    $valueY = Map-Y $values[$index] $minimum $maximum $plotTop $plotHeight
                    $top = [Math]::Min($baselineY, $valueY)
                    $barHeight = [Math]::Abs($baselineY - $valueY)
                    if ($barHeight -lt 1) {
                        $barHeight = 1
                    }
                    $graphics.FillRectangle($accentBrush, $x, $top, $barWidth, $barHeight)
                    $graphics.DrawRectangle($barPen, $x, $top, $barWidth, $barHeight)
                    $graphics.DrawString([string]$labels[$index], $labelFont, $axisBrush, $x - 8, $plotBottom + 12)
                    $graphics.DrawString(([string]::Format('{0:N2}', $values[$index])), $valueFont, $inkBrush, $x - 2, $top - 22)
                }
            }

            $directory = [System.IO.Path]::GetDirectoryName([string]$payload.output_path)
            if ($directory) {
                [System.IO.Directory]::CreateDirectory($directory) | Out-Null
            }
            $bitmap.Save([string]$payload.output_path, [System.Drawing.Imaging.ImageFormat]::Jpeg)
        }
        finally {
            $linePen.Dispose()
            $gridPen.Dispose()
            $barPen.Dispose()
            $axisBrush.Dispose()
            $inkBrush.Dispose()
            $accentBrush.Dispose()
            $titleFont.Dispose()
            $labelFont.Dispose()
            $valueFont.Dispose()
            $graphics.Dispose()
            $bitmap.Dispose()
        }
        '''
    ).strip()



def _render_chart_jpg(mode: str, output_path: Path, title: str, labels: list[str], values: list[float], width: int, height: int) -> Path:
    if not labels or not values or len(labels) != len(values):
        raise ValueError("图表标签和值不能为空，且长度必须一致。")

    output_path = output_path.resolve()
    try:
        return _render_with_pillow(mode, output_path, title, labels, values, width, height)
    except Exception:
        pass

    payload = {
        "mode": mode,
        "title": title,
        "labels": labels,
        "values": values,
        "width": width,
        "height": height,
        "output_path": str(output_path),
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    token = uuid.uuid4().hex[:10]
    json_path = output_path.parent / f"_chart_{token}.json"
    script_path = output_path.parent / f"_chart_{token}.ps1"
    try:
        json_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        script_path.write_text(_chart_script(), encoding="utf-8")
        process = subprocess.run(
            [_powershell_path(), "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(script_path), str(json_path)],
            check=False,
            capture_output=True,
            text=True,
        )
    finally:
        json_path.unlink(missing_ok=True)
        script_path.unlink(missing_ok=True)
    if process.returncode != 0:
        error_text = (process.stderr or process.stdout or "").strip()
        raise RuntimeError(f"JPG 图表生成失败: {error_text}")
    return output_path



def write_line_chart_jpg(
    output_path: Path,
    title: str,
    labels: list[str],
    values: list[float],
    width: int = 1280,
    height: int = 720,
) -> Path:
    return _render_chart_jpg("line", output_path, title, labels, values, width, height)



def write_bar_chart_jpg(
    output_path: Path,
    title: str,
    labels: list[str],
    values: list[float],
    width: int = 1280,
    height: int = 720,
) -> Path:
    return _render_chart_jpg("bar", output_path, title, labels, values, width, height)
