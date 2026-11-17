use crate::config::Config;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize)]
pub struct Profile {
    pub id: String,
    pub available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip)]
    pub(crate) arguments: Arc<[String]>,
    #[serde(skip)]
    pub(crate) probe_arguments: Vec<String>,
    #[serde(skip)]
    pub(crate) vaapi: bool,
    #[serde(skip)]
    pub(crate) copy_aac: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Definition {
    decoder_threads: u8,
    video: Video,
    audio: Audio,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "encoder", deny_unknown_fields)]
enum Video {
    #[serde(rename = "libx264")]
    X264 {
        preset: Preset,
        crf: u8,
        threads: u8,
    },
    #[serde(rename = "h264_vaapi")]
    Vaapi { qp: u8, threads: u8 },
    #[serde(rename = "libsvtav1")]
    SvtAv1 {
        crf: u8,
        preset: u8,
        logical_processors: u8,
    },
    #[serde(rename = "libx265")]
    X265 {
        crf: u8,
        preset: Preset,
        pools: u8,
        frame_threads: u8,
    },
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Preset {
    Ultrafast,
    Superfast,
    Veryfast,
    Faster,
    Fast,
    Medium,
    Slow,
    Slower,
    Veryslow,
    Placebo,
}
impl Preset {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ultrafast => "ultrafast",
            Self::Superfast => "superfast",
            Self::Veryfast => "veryfast",
            Self::Faster => "faster",
            Self::Fast => "fast",
            Self::Medium => "medium",
            Self::Slow => "slow",
            Self::Slower => "slower",
            Self::Veryslow => "veryslow",
            Self::Placebo => "placebo",
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum Audio {
    Aac {
        bitrate_kbps: u16,
        streams: AudioStreams,
    },
    CopyAac {
        streams: AudioStreams,
    },
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AudioStreams {
    All,
    First,
}

pub(crate) fn valid_id(id: &str) -> bool {
    (1..=64).contains(&id.len())
        && id
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

fn range(name: &str, value: u16, min: u16, max: u16) -> Result<(), String> {
    if (min..=max).contains(&value) {
        Ok(())
    } else {
        Err(format!("{name} must be between {min} and {max}"))
    }
}

impl Definition {
    fn validate(&self) -> Result<(), String> {
        range("decoder_threads", self.decoder_threads.into(), 1, 64)?;
        match &self.video {
            Video::X264 { crf, threads, .. } => {
                range("video.crf", (*crf).into(), 0, 51)?;
                range("video.threads", (*threads).into(), 1, 64)?;
            }
            Video::Vaapi { qp, threads } => {
                range("video.qp", (*qp).into(), 0, 51)?;
                range("video.threads", (*threads).into(), 1, 64)?;
            }
            Video::SvtAv1 {
                crf,
                preset,
                logical_processors,
            } => {
                range("video.crf", (*crf).into(), 0, 63)?;
                range("video.preset", (*preset).into(), 0, 13)?;
                range(
                    "video.logical_processors",
                    (*logical_processors).into(),
                    1,
                    64,
                )?;
            }
            Video::X265 {
                crf,
                pools,
                frame_threads,
                ..
            } => {
                range("video.crf", (*crf).into(), 0, 51)?;
                range("video.pools", (*pools).into(), 1, 64)?;
                range("video.frame_threads", (*frame_threads).into(), 1, 16)?;
            }
        }
        if let Audio::Aac { bitrate_kbps, .. } = &self.audio {
            range("audio.bitrate_kbps", *bitrate_kbps, 8, 512)?;
        }
        Ok(())
    }

    fn video_arguments(&self) -> Vec<String> {
        let mut args = Vec::new();
        match &self.video {
            Video::X264 {
                preset,
                crf,
                threads,
            } => {
                extend(
                    &mut args,
                    &[
                        "-c:v",
                        "libx264",
                        "-preset",
                        preset.as_str(),
                        "-crf",
                        &crf.to_string(),
                        "-threads",
                        &threads.to_string(),
                    ],
                );
            }
            Video::Vaapi { qp, threads } => {
                extend(
                    &mut args,
                    &[
                        "-vf",
                        "format=nv12,hwupload",
                        "-c:v",
                        "h264_vaapi",
                        "-qp",
                        &qp.to_string(),
                        "-threads",
                        &threads.to_string(),
                    ],
                );
            }
            Video::SvtAv1 {
                crf,
                preset,
                logical_processors,
            } => {
                extend(
                    &mut args,
                    &[
                        "-c:v",
                        "libsvtav1",
                        "-crf",
                        &crf.to_string(),
                        "-preset",
                        &preset.to_string(),
                        "-svtav1-params",
                        &format!("lp={logical_processors}"),
                    ],
                );
            }
            Video::X265 {
                crf,
                preset,
                pools,
                frame_threads,
            } => {
                extend(
                    &mut args,
                    &[
                        "-c:v",
                        "libx265",
                        "-crf",
                        &crf.to_string(),
                        "-preset",
                        preset.as_str(),
                        "-x265-params",
                        &format!("pools={pools}:frame-threads={frame_threads}"),
                    ],
                );
            }
        }
        if !matches!(self.video, Video::Vaapi { .. }) {
            extend(&mut args, &["-pix_fmt", "yuv420p"]);
        }
        // The default 1/fps encoder timebase rounds sub-frame input offsets,
        // shifting video against copied AAC. Preserve the source timebase instead.
        extend(&mut args, &["-enc_time_base:v", "-1"]);
        args
    }

    fn compile(self, id: String, device: &str) -> Profile {
        let vaapi = matches!(self.video, Video::Vaapi { .. });
        let copy_aac = matches!(self.audio, Audio::CopyAac { .. });
        let mut prefix = Vec::new();
        extend(
            &mut prefix,
            &[
                "-hide_banner",
                "-nostdin",
                "-loglevel",
                "warning",
                "-nostats",
                "-filter_threads",
                "1",
            ],
        );
        if vaapi {
            extend(&mut prefix, &["-vaapi_device", device]);
        }
        let mut arguments = prefix.clone();
        // No recipe can add filesystem/network inputs, demuxer options or output paths.
        extend(
            &mut arguments,
            &[
                "-protocol_whitelist",
                "pipe",
                "-threads",
                &self.decoder_threads.to_string(),
                "-i",
                "pipe:0",
                "-map",
                "0:v:0",
                "-map",
            ],
        );
        let streams = match self.audio {
            Audio::Aac { streams, .. } | Audio::CopyAac { streams } => streams,
        };
        arguments.push(
            match streams {
                AudioStreams::All => "0:a?",
                AudioStreams::First => "0:a:0?",
            }
            .into(),
        );
        let video = self.video_arguments();
        arguments.extend(video.iter().cloned());
        let mut audio = Vec::new();
        match self.audio {
            Audio::Aac { bitrate_kbps, .. } => extend(
                &mut audio,
                &["-c:a", "aac", "-b:a", &format!("{bitrate_kbps}k")],
            ),
            // This AAC-only filter rejects non-AAC before muxing, without decoding.
            Audio::CopyAac { .. } => {
                extend(&mut audio, &["-c:a", "copy", "-bsf:a", "aac_adtstoasc"])
            }
        }
        arguments.extend(audio.iter().cloned());
        extend(&mut arguments, &["-f", "matroska", "pipe:1"]);

        let mut probe_arguments = prefix;
        extend(
            &mut probe_arguments,
            &[
                "-threads",
                &self.decoder_threads.to_string(),
                "-f",
                "lavfi",
                "-i",
                "testsrc2=size=128x128:rate=10",
            ],
        );
        if !copy_aac {
            extend(
                &mut probe_arguments,
                &[
                    "-f",
                    "lavfi",
                    "-i",
                    "sine=frequency=1000:sample_rate=48000",
                    "-map",
                    "0:v:0",
                    "-map",
                    "1:a:0",
                ],
            );
        }
        extend(&mut probe_arguments, &["-frames:v", "2", "-t", "0.2"]);
        probe_arguments.extend(video);
        if copy_aac {
            extend(&mut probe_arguments, &["-an"]);
        } else {
            probe_arguments.extend(audio);
        }
        extend(&mut probe_arguments, &["-f", "matroska", "pipe:1"]);
        Profile {
            id,
            available: true,
            reason: None,
            arguments: arguments.into(),
            probe_arguments,
            vaapi,
            copy_aac,
        }
    }
}

fn extend(args: &mut Vec<String>, values: &[&str]) {
    args.extend(values.iter().map(|value| (*value).to_owned()));
}

/// Load only enabled recipes, once before listening. Missing files never fall back.
pub fn load(config: &Config) -> Result<Vec<Profile>, String> {
    config.validate()?;
    config
        .profiles
        .enabled
        .iter()
        .map(|id| {
            let path = config.profiles.directory.join(format!("{id}.toml"));
            let source = std::fs::read_to_string(&path)
                .map_err(|error| format!("profile {}: {error}", path.display()))?;
            let definition: Definition = toml::from_str(&source)
                .map_err(|error| format!("profile {}: {error}", path.display()))?;
            definition
                .validate()
                .map_err(|error| format!("profile {}: {error}", path.display()))?;
            Ok(definition.compile(id.clone(), &config.ffmpeg.vaapi_device))
        })
        .collect()
}

#[cfg(test)]
pub(crate) fn test_profile() -> Profile {
    let definition: Definition = toml::from_str("decoder_threads = 2\n[video]\nencoder = 'libx264'\npreset = 'veryfast'\ncrf = 23\nthreads = 2\n[audio]\nmode = 'aac'\nbitrate_kbps = 128\nstreams = 'all'\n").unwrap();
    definition.validate().unwrap();
    definition.compile("h264-cpu-mkv".into(), "/dev/dri/renderD128")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    const CPU: &str = "decoder_threads = 2\n[video]\nencoder = 'libx264'\npreset = 'veryfast'\ncrf = 23\nthreads = 2\n[audio]\nmode = 'aac'\nbitrate_kbps = 128\nstreams = 'all'\n";
    const VAAPI: &str = "decoder_threads = 2\n[video]\nencoder = 'h264_vaapi'\nqp = 23\nthreads = 2\n[audio]\nmode = 'aac'\nbitrate_kbps = 128\nstreams = 'all'\n";
    const AV1: &str = "decoder_threads = 2\n[video]\nencoder = 'libsvtav1'\ncrf = 30\npreset = 6\nlogical_processors = 12\n[audio]\nmode = 'copy_aac'\nstreams = 'first'\n";
    const HEVC: &str = "decoder_threads = 2\n[video]\nencoder = 'libx265'\ncrf = 26\npreset = 'slow'\npools = 12\nframe_threads = 3\n[audio]\nmode = 'copy_aac'\nstreams = 'first'\n";

    fn parse(source: &str) -> Result<Definition, String> {
        let definition: Definition = toml::from_str(source).map_err(|error| error.to_string())?;
        definition.validate()?;
        Ok(definition)
    }

    fn value<'a>(args: &'a [String], flag: &str) -> &'a str {
        &args[args.iter().position(|arg| arg == flag).unwrap() + 1]
    }

    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("transcoder-profiles-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }

        fn config(&self, id: &str) -> Config {
            let mut config = Config::default();
            config.profiles.directory = self.0.clone();
            config.profiles.enabled = vec![id.into()];
            config
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn all_variants_compile_restricted_streaming_commands() {
        for source in [CPU, VAAPI, AV1, HEVC] {
            let profile = parse(source)
                .unwrap()
                .compile("custom-recipe".into(), "/dev/dri/renderD129");
            let args = &profile.arguments;
            assert_eq!(value(args, "-protocol_whitelist"), "pipe");
            assert_eq!(value(args, "-i"), "pipe:0");
            assert!(
                args.iter().position(|arg| arg == "-protocol_whitelist")
                    < args.iter().position(|arg| arg == "-i")
            );
            assert_eq!(value(args, "-f"), "matroska");
            assert_eq!(args.last().unwrap(), "pipe:1");
            assert!(
                !args
                    .iter()
                    .any(|arg| ["-r", "-s", "-t", "-movflags", "-tag:v"].contains(&arg.as_str()))
            );
            assert_eq!(value(args, "-threads"), "2");
            assert_eq!(value(args, "-enc_time_base:v"), "-1");
            assert_eq!(value(&profile.probe_arguments, "-enc_time_base:v"), "-1");
            if profile.vaapi {
                assert_eq!(value(args, "-vaapi_device"), "/dev/dri/renderD129");
                assert_eq!(value(args, "-vf"), "format=nv12,hwupload");
                assert!(!args.iter().any(|arg| arg == "-pix_fmt"));
            } else {
                assert_eq!(value(args, "-pix_fmt"), "yuv420p");
                assert!(!args.iter().any(|arg| arg == "-vf"));
            }
            assert_eq!(value(args, "-c:v"), value(&profile.probe_arguments, "-c:v"));
            if profile.copy_aac {
                assert_eq!(value(args, "-c:a"), "copy");
                assert_eq!(value(args, "-bsf:a"), "aac_adtstoasc");
                assert!(args.iter().any(|arg| arg == "0:a:0?"));
                assert!(!args.iter().any(|arg| arg == "-b:a"));
                // AV1/HEVC threading is encoder-specific, not a generic output option.
                assert_eq!(
                    args.iter().filter(|arg| arg.as_str() == "-threads").count(),
                    1
                );
            } else {
                assert_eq!(value(args, "-c:a"), "aac");
                assert_eq!(value(args, "-b:a"), "128k");
                assert!(args.iter().any(|arg| arg == "0:a?"));
            }
            for flag in ["-crf", "-preset", "-qp", "-svtav1-params", "-x265-params"] {
                if args.iter().any(|arg| arg == flag) {
                    assert_eq!(value(args, flag), value(&profile.probe_arguments, flag));
                }
            }
        }
        let av1 = parse(AV1).unwrap().compile("av1".into(), "");
        assert_eq!(value(&av1.arguments, "-crf"), "30");
        assert_eq!(value(&av1.arguments, "-preset"), "6");
        assert_eq!(value(&av1.arguments, "-svtav1-params"), "lp=12");
        let hevc = parse(HEVC).unwrap().compile("hevc".into(), "");
        assert_eq!(value(&hevc.arguments, "-crf"), "26");
        assert_eq!(value(&hevc.arguments, "-preset"), "slow");
        assert_eq!(
            value(&hevc.arguments, "-x265-params"),
            "pools=12:frame-threads=3"
        );
    }

    #[test]
    fn parser_rejects_unknown_and_inappropriate_variant_fields() {
        for source in [
            format!("output = 'file:///tmp/output'\n{CPU}"),
            format!("{CPU}\n[output]\nformat = 'mp4'"),
            CPU.replace("encoder = 'libx264'", "encoder = 'arbitrary'"),
            CPU.replace("preset = 'veryfast'", "preset = '-i file:///etc/passwd'"),
            CPU.replace("threads = 2\n[audio]", "threads = 2\npools = 12\n[audio]"),
            CPU.replace("streams = 'all'", "streams = 'none'"),
            CPU.replace("mode = 'aac'", "mode = 'copy'"),
            format!("{AV1}bitrate_kbps = 128\n"),
            AV1.replace("preset = 6", "preset = '6'"),
            HEVC.replace("pools = 12", "logical_processors = 12"),
            VAAPI.replace("qp = 23", "crf = 23"),
            CPU.replace("decoder_threads = 2\n", ""),
            format!("{CPU}\nargs = ['-r', '60']"),
        ] {
            assert!(
                parse(&source).is_err(),
                "accepted unsafe/invalid recipe: {source}"
            );
        }
    }

    #[test]
    fn numeric_ranges_are_checked_at_every_boundary() {
        for (source, field, initial, min, max) in [
            (CPU, "decoder_threads", 2, 1, 64),
            (CPU, "crf", 23, 0, 51),
            (CPU, "threads", 2, 1, 64),
            (VAAPI, "qp", 23, 0, 51),
            (VAAPI, "threads", 2, 1, 64),
            (AV1, "crf", 30, 0, 63),
            (AV1, "preset", 6, 0, 13),
            (AV1, "logical_processors", 12, 1, 64),
            (HEVC, "crf", 26, 0, 51),
            (HEVC, "pools", 12, 1, 64),
            (HEVC, "frame_threads", 3, 1, 16),
            (CPU, "bitrate_kbps", 128, 8, 512),
        ] {
            for number in [min - 1, min, max, max + 1] {
                let recipe = source.replace(
                    &format!("\n{field} = {initial}\n"),
                    &format!("\n{field} = {number}\n"),
                );
                // decoder_threads is the first field, without a leading newline.
                let recipe = if field == "decoder_threads" {
                    recipe.replacen(
                        "decoder_threads = 2",
                        &format!("decoder_threads = {number}"),
                        1,
                    )
                } else {
                    recipe
                };
                assert_eq!(
                    parse(&recipe).is_ok(),
                    (min..=max).contains(&number),
                    "{field} = {number}"
                );
            }
        }
    }

    #[test]
    fn ids_and_duplicate_enabled_entries_are_rejected_before_lookup() {
        let directory = Directory::new();
        for id in [
            "",
            ".",
            "..",
            "../escape",
            "/absolute",
            "a/b",
            "a\\\\b",
            "Upper",
            "-leading",
            "é",
            "a.toml",
            "a b",
        ] {
            assert!(load(&directory.config(id)).is_err(), "{id:?}");
        }
        assert!(load(&directory.config(&"a".repeat(65))).is_err());
        assert!(valid_id(&"a".repeat(64)));
        assert!(valid_id("0-custom-av1"));
        let mut config = directory.config("custom");
        config.profiles.enabled.push("custom".into());
        assert!(load(&config).unwrap_err().contains("duplicate"));
        config.profiles.enabled.clear();
        assert!(load(&config).is_err());
        config.profiles.directory = PathBuf::new();
        assert!(load(&config).unwrap_err().contains("directory"));
    }

    #[test]
    fn only_enabled_files_are_loaded_with_no_embedded_fallback() {
        let directory = Directory::new();
        let config = directory.config("h264-cpu-mkv");
        let error = load(&config).unwrap_err();
        assert!(error.contains(&directory.0.join("h264-cpu-mkv.toml").display().to_string()));
        std::fs::write(directory.0.join("custom.toml"), HEVC).unwrap();
        std::fs::write(directory.0.join("disabled.toml"), "not valid TOML").unwrap();
        let config = directory.config("custom");
        let loaded = load(&config).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, "custom");
        assert_eq!(value(&loaded[0].arguments, "-c:v"), "libx265");
        let retained = loaded[0].arguments.clone();
        std::fs::write(
            directory.0.join("custom.toml"),
            HEVC.replace("crf = 26", "crf = 31"),
        )
        .unwrap();
        assert_eq!(value(&retained, "-crf"), "26");
        assert_eq!(value(&load(&config).unwrap()[0].arguments, "-crf"), "31");
        std::fs::write(directory.0.join("custom.toml"), "unknown = true").unwrap();
        let error = load(&config).unwrap_err();
        assert!(error.contains("custom.toml"));
        assert!(error.contains("unknown"));
    }

    #[test]
    fn profile_directory_is_relative_to_config_parent_not_working_directory() {
        let directory = Directory::new();
        let recipes = directory.0.join("recipes");
        std::fs::create_dir(&recipes).unwrap();
        std::fs::write(recipes.join("custom.toml"), AV1).unwrap();
        let path = directory.0.join("server.toml");
        std::fs::write(
            &path,
            "[profiles]\ndirectory = 'recipes'\nenabled = ['custom']",
        )
        .unwrap();
        let config = Config::load(Some(&path)).unwrap();
        assert_eq!(config.profiles.directory, recipes);
        assert_eq!(load(&config).unwrap()[0].id, "custom");
        std::fs::write(&path, "[profiles]\ndirectory = ''").unwrap();
        let error = Config::load(Some(&path)).unwrap_err().to_string();
        assert!(error.contains("server.toml"));
        assert!(error.contains("directory"));
        std::fs::write(&path, "[profiles]\nunknown = 'ignored?'").unwrap();
        assert!(Config::load(Some(&path)).is_err());
        std::fs::write(&path, "").unwrap();
        assert_eq!(
            Config::load(Some(&path)).unwrap().profiles.directory,
            directory.0.join("profiles")
        );
        assert_eq!(
            Config::load(None).unwrap().profiles.directory,
            std::env::current_dir().unwrap().join("profiles")
        );
        std::fs::write(
            &path,
            format!(
                "[profiles]\ndirectory = '{}'\nenabled = ['custom']",
                recipes.display()
            ),
        )
        .unwrap();
        let absolute = Config::load(Some(&path)).unwrap();
        assert_eq!(absolute.profiles.directory, recipes);
        assert_eq!(load(&absolute).unwrap()[0].id, "custom");
    }

    #[test]
    fn audio_selection_is_independent_of_encoder_and_copy_mode() {
        let all = parse(&AV1.replace("streams = 'first'", "streams = 'all'"))
            .unwrap()
            .compile("all-aac".into(), "");
        assert!(all.arguments.iter().any(|arg| arg == "0:a?"));
        assert_eq!(value(&all.arguments, "-bsf:a"), "aac_adtstoasc");
        let first = parse(&CPU.replace("streams = 'all'", "streams = 'first'"))
            .unwrap()
            .compile("first-audio".into(), "");
        assert!(first.arguments.iter().any(|arg| arg == "0:a:0?"));
        assert_eq!(value(&first.arguments, "-c:a"), "aac");
    }

    #[test]
    fn serialized_profiles_do_not_expose_internal_recipes() {
        let profile = test_profile();
        assert_eq!(
            serde_json::to_value(&profile).unwrap(),
            serde_json::json!({
                "id": "h264-cpu-mkv", "available": true
            })
        );
    }
}
