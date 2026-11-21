

// Include the cxx-generated header first (contains struct definitions)
#include "libtorrent-sys/cpp/wrapper.h"
#include "libtorrent-sys/cpp/memory_storage.hpp"
#include "libtorrent-sys/src/lib.rs.h"

#include <libtorrent/add_torrent_params.hpp>
#include <libtorrent/alert_types.hpp>
#include <libtorrent/announce_entry.hpp>
#include <libtorrent/aux_/session_impl.hpp>
#include <libtorrent/extensions.hpp>
#include <libtorrent/extensions/smart_ban.hpp>
#include <libtorrent/extensions/ut_metadata.hpp>
#include <libtorrent/extensions/ut_pex.hpp>
#include <libtorrent/hex.hpp>
#include <libtorrent/load_torrent.hpp>
#include <libtorrent/magnet_uri.hpp>
#include <libtorrent/peer_info.hpp>
#include <libtorrent/read_resume_data.hpp>
#include <libtorrent/session_params.hpp>
#include <libtorrent/session_stats.hpp>
#include <libtorrent/torrent_info.hpp>
#include <libtorrent/version.hpp>
#include <libtorrent/write_resume_data.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <iterator>
#include <sstream>
#include <utility>

namespace libtorrent_wrapper {

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

static rust::String sha1_to_hex(const lt::sha1_hash &hash) {
  std::stringstream ss;
  ss << hash;
  return rust::String(ss.str());
}

static rust::String sha256_to_hex(const lt::sha256_hash &hash) {
  std::stringstream ss;
  ss << hash;
  return rust::String(ss.str());
}

static lt::sha1_hash hex_to_sha1(rust::Str hex) {
  std::string hex_str(hex.data(), hex.size());
  lt::sha1_hash hash;
  lt::aux::from_hex(hex_str, hash.data());
  return hash;
}

static std::string rust_str_to_std(rust::Str s) {
  return std::string(s.data(), s.size());
}

static std::vector<std::shared_ptr<lt::plugin>>
session_plugins(bool enable_pex) {
#ifndef TORRENT_DISABLE_EXTENSIONS
  using wrapper = lt::aux::session_impl::session_plugin_wrapper;
  std::vector<std::shared_ptr<lt::plugin>> plugins;
  if (enable_pex) {
    plugins.push_back(std::make_shared<wrapper>(lt::create_ut_pex_plugin));
  }
  plugins.push_back(std::make_shared<wrapper>(lt::create_ut_metadata_plugin));
  plugins.push_back(std::make_shared<wrapper>(lt::create_smart_ban_plugin));
  return plugins;
#else
  (void)enable_pex;
  return {};
#endif
}

static void apply_privacy_settings(lt::settings_pack &pack,
                                   SessionSettings const &settings) {
  pack.set_bool(lt::settings_pack::enable_dht, settings.enable_dht);
  pack.set_bool(lt::settings_pack::enable_lsd, settings.enable_lsd);
  pack.set_bool(lt::settings_pack::anonymous_mode, settings.anonymous_mode);
  pack.set_bool(lt::settings_pack::allow_multiple_connections_per_ip,
                settings.allow_multiple_connections_per_ip);
  pack.set_bool(lt::settings_pack::validate_https_trackers,
                settings.validate_https_trackers);
  pack.set_bool(lt::settings_pack::ssrf_mitigation, settings.ssrf_mitigation);

  switch (settings.encryption_mode) {
  case 1:
    pack.set_int(lt::settings_pack::out_enc_policy,
                 lt::settings_pack::pe_forced);
    pack.set_int(lt::settings_pack::in_enc_policy,
                 lt::settings_pack::pe_forced);
    pack.set_int(lt::settings_pack::allowed_enc_level,
                 lt::settings_pack::pe_both);
    break;
  case 2:
    pack.set_int(lt::settings_pack::out_enc_policy,
                 lt::settings_pack::pe_disabled);
    pack.set_int(lt::settings_pack::in_enc_policy,
                 lt::settings_pack::pe_disabled);
    pack.set_int(lt::settings_pack::allowed_enc_level,
                 lt::settings_pack::pe_plaintext);
    break;
  default:
    pack.set_int(lt::settings_pack::out_enc_policy,
                 lt::settings_pack::pe_enabled);
    pack.set_int(lt::settings_pack::in_enc_policy,
                 lt::settings_pack::pe_enabled);
    pack.set_int(lt::settings_pack::allowed_enc_level,
                 lt::settings_pack::pe_both);
    break;
  }
}

static void apply_network_settings(lt::settings_pack &pack,
                                   SessionSettings const &settings) {
  if (!settings.listen_interfaces.empty()) {
    pack.set_str(lt::settings_pack::listen_interfaces,
                 rust_str_to_std(settings.listen_interfaces));
  } else {
    pack.set_str(lt::settings_pack::listen_interfaces,
                 "0.0.0.0:42000-42010,[::]:42000-42010");
  }

  pack.set_str(lt::settings_pack::outgoing_interfaces,
               rust_str_to_std(settings.outgoing_interfaces));
  pack.set_int(lt::settings_pack::outgoing_port, settings.outgoing_port);
  pack.set_int(lt::settings_pack::num_outgoing_ports,
               settings.num_outgoing_ports);
}

static void set_bool_setting_if_supported(lt::settings_pack &pack,
                                          char const *name, bool value) {
  int const setting = lt::setting_by_name(name);
  if (setting >= 0) {
    pack.set_bool(setting, value);
  }
}

static void apply_proxy_settings(lt::settings_pack &pack,
                                 SessionSettings const &settings) {
  pack.set_int(lt::settings_pack::proxy_type, settings.proxy_type);
  pack.set_str(lt::settings_pack::proxy_hostname,
               rust_str_to_std(settings.proxy_host));
  pack.set_int(lt::settings_pack::proxy_port, settings.proxy_port);
  pack.set_str(lt::settings_pack::proxy_username,
               rust_str_to_std(settings.proxy_username));
  pack.set_str(lt::settings_pack::proxy_password,
               rust_str_to_std(settings.proxy_password));
  pack.set_bool(lt::settings_pack::proxy_hostnames, settings.proxy_hostnames);
  pack.set_bool(lt::settings_pack::proxy_peer_connections,
                settings.proxy_peer_connections);
  pack.set_bool(lt::settings_pack::proxy_tracker_connections,
                settings.proxy_tracker_connections);
  set_bool_setting_if_supported(pack, "proxy_send_host_in_connect",
                                settings.proxy_send_host_in_connect);
}

static lt::session_params make_session_params(lt::settings_pack pack,
                                              SessionSettings const &settings) {
  return lt::session_params(std::move(pack),
                            session_plugins(settings.enable_pex));
}

static lt::status_flags_t playback_status_flags() {
  // The remaining status fields are cheap. In particular, omit piece maps,
  // verified-piece maps, distributed-copy calculation and torrent metadata.
  return lt::torrent_handle::query_name |
         lt::torrent_handle::query_save_path |
         lt::torrent_handle::query_last_seen_complete;
}

static void apply_streaming_performance_settings(lt::settings_pack &pack) {
  // Keep upload buffers large enough to maintain reciprocity without forcing
  // OS socket-buffer sizes and defeating TCP auto-tuning.
  pack.set_int(lt::settings_pack::send_buffer_watermark, 512 * 1024);
  pack.set_int(lt::settings_pack::send_buffer_watermark_factor, 150);
  pack.set_int(lt::settings_pack::send_buffer_low_watermark, 10 * 1024);

  pack.set_int(lt::settings_pack::choking_algorithm, 0);
  pack.set_int(lt::settings_pack::seed_choking_algorithm, 2);

  // Favor fast swarm acquisition while leaving enough time for useful peers
  // on higher-latency paths to complete their TCP handshake.
  pack.set_int(lt::settings_pack::request_timeout, 10);
  pack.set_int(lt::settings_pack::peer_timeout, 60);
  pack.set_int(lt::settings_pack::min_reconnect_time, 1);
  pack.set_int(lt::settings_pack::max_failcount, 3);
  pack.set_int(lt::settings_pack::connection_speed, 200);
  pack.set_int(lt::settings_pack::peer_connect_timeout, 7);
  pack.set_int(lt::settings_pack::torrent_connect_boost, 255);

  pack.set_str(lt::settings_pack::dht_bootstrap_nodes,
               "router.bittorrent.com:6881,"
               "router.utorrent.com:6881,"
               "dht.transmissionbt.com:6881,"
               "dht.libtorrent.org:25401,"
               "router.bitcomet.com:6881");

  // Deadline pieces drive playback. Strict end-game avoids waste before the
  // true end game; libtorrent's time-critical picker handles adaptive duplicate
  // requests for stalled deadline blocks.
  pack.set_bool(lt::settings_pack::strict_end_game_mode, true);
  pack.set_bool(lt::settings_pack::prioritize_partial_pieces, true);
  pack.set_bool(lt::settings_pack::smooth_connects, false);
  pack.set_int(lt::settings_pack::piece_timeout, 5);

  // 2.1 raised these defaults. The larger outbound ceiling is also used by
  // libtorrent's high-performance preset and avoids clipping high-BDP peers;
  // the inbound value preserves enough upload pipeline for reciprocity.
  pack.set_int(lt::settings_pack::max_out_request_queue, 1500);
  pack.set_int(lt::settings_pack::max_allowed_in_request_queue, 2000);
  pack.set_int(lt::settings_pack::request_queue_time, 3);
  pack.set_int(lt::settings_pack::unchoke_slots_limit, 20);
  pack.set_int(lt::settings_pack::alert_queue_size, 10000);
}

static TorrentStatus make_torrent_status(const lt::torrent_status &ts) {
  TorrentStatus status;
  status.info_hash = sha1_to_hex(ts.info_hashes.get_best());
  status.info_hash_v2 = ts.info_hashes.has_v2()
                            ? sha256_to_hex(ts.info_hashes.v2)
                            : rust::String("");
  status.name = rust::String(ts.name);
  status.save_path = rust::String(ts.save_path);
  status.state = static_cast<int32_t>(ts.state);
  status.total_size = ts.total;
  status.total_done = ts.total_done;
  status.total_downloaded = ts.all_time_download;
  status.total_uploaded = ts.all_time_upload;
  status.total_wanted = ts.total_wanted;
  status.total_wanted_done = ts.total_wanted_done;
  status.download_rate = ts.download_rate;
  status.upload_rate = ts.upload_rate;
  status.download_payload_rate = ts.download_payload_rate;
  status.upload_payload_rate = ts.upload_payload_rate;
  status.num_peers = ts.num_peers;
  status.num_seeds = ts.num_seeds;
  status.num_incomplete = ts.num_incomplete;
  status.num_complete = ts.num_complete;
  status.progress = ts.progress;
  status.progress_ppm = ts.progress_ppm;
  status.is_paused = bool(ts.flags & lt::torrent_flags::paused);
  status.is_auto_managed = bool(ts.flags & lt::torrent_flags::auto_managed);
  status.is_finished = ts.is_finished;
  status.is_seeding = ts.is_seeding;
  status.has_metadata = ts.has_metadata;
  status.sequential_download =
      bool(ts.flags & lt::torrent_flags::sequential_download);
  status.current_tracker = rust::String(ts.current_tracker);
  status.next_announce_seconds = static_cast<int32_t>(
      std::chrono::duration_cast<std::chrono::seconds>(ts.next_announce)
          .count());
  status.num_pieces = ts.num_pieces;
  // piece_length is in torrent_info, not torrent_status
  status.piece_length = 0;
  status.added_time = ts.added_time;
  status.completed_time = ts.completed_time;
  status.last_seen_complete = ts.last_seen_complete;
  status.queue_position =
      static_cast<int32_t>(static_cast<int>(ts.queue_position));
  status.error = ts.errc ? rust::String(ts.errc.message()) : rust::String("");
  return status;
}

// ============================================================================
// SESSION LIFECYCLE
// ============================================================================

std::unique_ptr<Session> create_session(SessionSettings const &settings) {
  lt::settings_pack pack;

  apply_network_settings(pack, settings);

  if (!settings.user_agent.empty()) {
    pack.set_str(lt::settings_pack::user_agent,
                 rust_str_to_std(settings.user_agent));
  }

  apply_privacy_settings(pack, settings);
  pack.set_bool(lt::settings_pack::enable_upnp, settings.enable_upnp);
  pack.set_bool(lt::settings_pack::enable_natpmp, settings.enable_natpmp);
  pack.set_bool(lt::settings_pack::announce_to_all_trackers,
                settings.announce_to_all_trackers);
  pack.set_bool(lt::settings_pack::announce_to_all_tiers,
                settings.announce_to_all_tiers);

  // Connection limits
  if (settings.max_connections > 0)
    pack.set_int(lt::settings_pack::connections_limit,
                 settings.max_connections);

  // Rate limits
  if (settings.download_rate_limit > 0)
    pack.set_int(lt::settings_pack::download_rate_limit,
                 settings.download_rate_limit);
  if (settings.upload_rate_limit > 0)
    pack.set_int(lt::settings_pack::upload_rate_limit,
                 settings.upload_rate_limit);

  // Active torrent limits
  if (settings.active_downloads > 0)
    pack.set_int(lt::settings_pack::active_downloads,
                 settings.active_downloads);
  if (settings.active_seeds > 0)
    pack.set_int(lt::settings_pack::active_seeds, settings.active_seeds);
  if (settings.active_limit > 0)
    pack.set_int(lt::settings_pack::active_limit, settings.active_limit);

  apply_streaming_performance_settings(pack);

  // Stay above 2.1.1's 100 MiB default so bursty disk writes do not throttle
  // fast peers. Extent affinity improves locality for torrents with small
  // pieces, and no-atime removes metadata writes on platforms that support it.
  pack.set_int(lt::settings_pack::max_queued_disk_bytes,
               128 * 1024 * 1024);
  pack.set_int(lt::settings_pack::aio_threads, 16);
  pack.set_int(lt::settings_pack::whole_pieces_threshold, 30);
  pack.set_bool(lt::settings_pack::piece_extent_affinity, true);
  pack.set_bool(lt::settings_pack::no_atime_storage, true);

  // Playback needs storage/control alerts plus piece_finished_alert, whose
  // category is piece_progress in libtorrent 2.x. Peer alerts are intentionally
  // omitted from this latency-sensitive pump.
  pack.set_int(lt::settings_pack::alert_mask,
               lt::alert_category::error | lt::alert_category::status |
                   lt::alert_category::storage |
                   lt::alert_category::piece_progress |
                   lt::alert_category::performance_warning);

  apply_proxy_settings(pack, settings);

  lt::session_params params = make_session_params(std::move(pack), settings);
  return std::make_unique<Session>(std::move(params), settings.enable_pex);
}

std::unique_ptr<Session>
create_session_disk_backed(SessionSettings const &settings) {
  return create_session(settings);
}

std::unique_ptr<Session>
create_session_memory_only(SessionSettings const &settings) {
  lt::settings_pack pack;

  apply_network_settings(pack, settings);

  if (!settings.user_agent.empty()) {
    pack.set_str(lt::settings_pack::user_agent,
                 rust_str_to_std(settings.user_agent));
  }

  apply_privacy_settings(pack, settings);
  pack.set_bool(lt::settings_pack::enable_upnp, settings.enable_upnp);
  pack.set_bool(lt::settings_pack::enable_natpmp, settings.enable_natpmp);
  pack.set_bool(lt::settings_pack::announce_to_all_trackers,
                settings.announce_to_all_trackers);
  pack.set_bool(lt::settings_pack::announce_to_all_tiers,
                settings.announce_to_all_tiers);

  if (settings.max_connections > 0)
    pack.set_int(lt::settings_pack::connections_limit,
                 settings.max_connections);
  if (settings.download_rate_limit > 0)
    pack.set_int(lt::settings_pack::download_rate_limit,
                 settings.download_rate_limit);
  if (settings.upload_rate_limit > 0)
    pack.set_int(lt::settings_pack::upload_rate_limit,
                 settings.upload_rate_limit);
  if (settings.active_downloads > 0)
    pack.set_int(lt::settings_pack::active_downloads,
                 settings.active_downloads);
  if (settings.active_seeds > 0)
    pack.set_int(lt::settings_pack::active_seeds, settings.active_seeds);
  if (settings.active_limit > 0)
    pack.set_int(lt::settings_pack::active_limit, settings.active_limit);

  apply_streaming_performance_settings(pack);

  pack.set_int(lt::settings_pack::alert_mask,
               lt::alert_category::error | lt::alert_category::status |
                   lt::alert_category::storage |
                   lt::alert_category::piece_progress |
                   lt::alert_category::performance_warning);

  apply_proxy_settings(pack, settings);

  lt::session_params params = make_session_params(std::move(pack), settings);

  // CRITICAL: Use memory-only disk I/O constructor - no files written to disk
  params.disk_io_constructor = memory_disk_io_constructor;

  return std::make_unique<Session>(std::move(params), settings.enable_pex);
}

void session_abort(Session &session) { session.session.abort(); }

void session_pause(Session &session) { session.session.pause(); }

void session_resume(Session &session) { session.session.resume(); }

bool session_is_paused(Session const &session) {
  return session.session.is_paused();
}

// ============================================================================
// SESSION SETTINGS
// ============================================================================

void session_apply_settings(Session &session, SessionSettings const &settings) {
  lt::settings_pack pack;
  apply_network_settings(pack, settings);
  apply_privacy_settings(pack, settings);
  pack.set_bool(lt::settings_pack::enable_upnp, settings.enable_upnp);
  pack.set_bool(lt::settings_pack::enable_natpmp, settings.enable_natpmp);
  pack.set_bool(lt::settings_pack::announce_to_all_trackers,
                settings.announce_to_all_trackers);
  pack.set_bool(lt::settings_pack::announce_to_all_tiers,
                settings.announce_to_all_tiers);
  apply_proxy_settings(pack, settings);
  if (settings.download_rate_limit >= 0)
    pack.set_int(lt::settings_pack::download_rate_limit,
                 settings.download_rate_limit);
  if (settings.upload_rate_limit >= 0)
    pack.set_int(lt::settings_pack::upload_rate_limit,
                 settings.upload_rate_limit);
  session.session.apply_settings(pack);

#ifndef TORRENT_DISABLE_EXTENSIONS
  if (settings.enable_pex && !session.pex_extension_enabled) {
    session.session.add_extension(&lt::create_ut_pex_plugin);
    session.pex_extension_enabled = true;
  }
#else
  (void)settings;
#endif
}

int32_t session_get_download_rate_limit(Session const &session) {
  return session.session.get_settings().get_int(
      lt::settings_pack::download_rate_limit);
}

int32_t session_get_upload_rate_limit(Session const &session) {
  return session.session.get_settings().get_int(
      lt::settings_pack::upload_rate_limit);
}

void session_set_download_rate_limit(Session &session, int32_t limit) {
  lt::settings_pack pack;
  pack.set_int(lt::settings_pack::download_rate_limit, limit);
  session.session.apply_settings(pack);
}

void session_set_upload_rate_limit(Session &session, int32_t limit) {
  lt::settings_pack pack;
  pack.set_int(lt::settings_pack::upload_rate_limit, limit);
  session.session.apply_settings(pack);
}

// ============================================================================
// ADD/REMOVE TORRENTS
// ============================================================================

std::unique_ptr<TorrentHandle>
session_add_torrent(Session &session, AddTorrentParams const &params) {
  lt::add_torrent_params p;

  if (!params.magnet_uri.empty()) {
    lt::error_code ec;
    lt::parse_magnet_uri(rust_str_to_std(params.magnet_uri), p, ec);
    if (ec)
      throw std::runtime_error("Failed to parse magnet: " + ec.message());
  }

  if (!params.torrent_data.empty()) {
    lt::error_code ec;
    auto loaded = lt::load_torrent_buffer(
        {reinterpret_cast<const char *>(params.torrent_data.data()),
         static_cast<std::ptrdiff_t>(params.torrent_data.size())},
        ec, lt::load_torrent_limits{});
    if (ec)
      throw std::runtime_error("Failed to parse torrent: " + ec.message());
    p.ti = std::move(loaded.ti);
  }

  p.save_path = rust_str_to_std(params.save_path);

  if (!params.name.empty())
    p.name = rust_str_to_std(params.name);

  for (const auto &t : params.trackers)
    p.trackers.push_back(std::string(t.data(), t.size()));

  if (params.paused)
    p.flags |= lt::torrent_flags::paused;
  else
    p.flags &= ~lt::torrent_flags::paused;
  if (params.auto_managed)
    p.flags |= lt::torrent_flags::auto_managed;
  else
    p.flags &= ~lt::torrent_flags::auto_managed;
  if (params.sequential_download)
    p.flags |= lt::torrent_flags::sequential_download;
  else
    p.flags &= ~lt::torrent_flags::sequential_download;

  p.upload_limit = params.upload_limit;
  p.download_limit = params.download_limit;

  lt::torrent_handle h = session.session.add_torrent(p);
  if (!h.is_valid())
    throw std::runtime_error("Failed to add torrent");
  memory_label_last_unlabeled_storage(sha1_to_hex(h.info_hashes().get_best()));

  return std::make_unique<TorrentHandle>(std::move(h));
}

std::unique_ptr<TorrentHandle> session_add_magnet(Session &session,
                                                  rust::Str magnet_uri,
                                                  rust::Str save_path) {
  lt::add_torrent_params p;
  lt::error_code ec;
  lt::parse_magnet_uri(rust_str_to_std(magnet_uri), p, ec);
  if (ec)
    throw std::runtime_error("Failed to parse magnet: " + ec.message());

  p.save_path = rust_str_to_std(save_path);

  // Unlike session_add_torrent (which reads an explicit auto_managed param
  // from Rust), this path left add_torrent_params at its library default,
  // which includes torrent_flags::auto_managed. Every torrent created via a
  // magnet link -- i.e. every torrent the streaming routes ever create --
  // was therefore left under libtorrent's own session-wide queue/seed
  // fairness management, which can pause/resume it independently of (and in
  // direct conflict with) our explicit handle_pause/handle_resume calls. We
  // always want full manual control, so clear the flag at creation time.
  p.flags &= ~lt::torrent_flags::auto_managed;
  // Keep the magnet paused until a metadata or playback permit activates it.
  // This lets the Rust coordinator be the sole owner of resume ordering.
  p.flags |= lt::torrent_flags::paused;

  lt::torrent_handle h = session.session.add_torrent(p);
  if (!h.is_valid())
    throw std::runtime_error("Failed to add torrent");
  memory_label_last_unlabeled_storage(sha1_to_hex(h.info_hashes().get_best()));

  return std::make_unique<TorrentHandle>(std::move(h));
}

void session_remove_torrent(Session &session, TorrentHandle const &handle,
                            bool delete_files) {
  auto info_hash = sha1_to_hex(handle.handle.info_hashes().get_best());
  lt::remove_flags_t flags =
      delete_files ? lt::session::delete_files : lt::remove_flags_t{};
  session.session.remove_torrent(handle.handle, flags);
  memory_clear_torrent(info_hash);
}

// ============================================================================
// TORRENT QUERIES
// ============================================================================

rust::Vec<TorrentStatus> session_get_torrents(Session const &session) {
  rust::Vec<TorrentStatus> result;
  for (const auto &h : session.session.get_torrents()) {
    if (!h.is_valid())
      continue;
    result.push_back(make_torrent_status(h.status(playback_status_flags())));
  }
  return result;
}

std::unique_ptr<TorrentHandle> session_find_torrent(Session const &session,
                                                    rust::Str info_hash) {
  lt::sha1_hash hash = hex_to_sha1(info_hash);
  lt::torrent_handle h = session.session.find_torrent(hash);
  if (!h.is_valid())
    throw std::runtime_error("Torrent not found");
  return std::make_unique<TorrentHandle>(std::move(h));
}

TorrentStatus session_get_torrent_status(Session const &session,
                                         rust::Str info_hash) {
  lt::sha1_hash hash = hex_to_sha1(info_hash);
  lt::torrent_handle h = session.session.find_torrent(hash);
  if (!h.is_valid())
    throw std::runtime_error("Torrent not found");
  return make_torrent_status(h.status(playback_status_flags()));
}

// ============================================================================
// SESSION STATS
// ============================================================================

SessionStats session_get_stats(Session const &session) {
  SessionStats stats;
  auto handles = session.session.get_torrents();

  stats.num_torrents = static_cast<int32_t>(handles.size());
  stats.download_rate = 0;
  stats.upload_rate = 0;
  stats.total_download = 0;
  stats.total_upload = 0;
  stats.num_peers = 0;

  for (const auto &h : handles) {
    if (!h.is_valid())
      continue;
    auto ts = h.status(lt::status_flags_t{});
    stats.download_rate += ts.download_rate;
    stats.upload_rate += ts.upload_rate;
    stats.total_download += ts.all_time_download;
    stats.total_upload += ts.all_time_upload;
    stats.num_peers += ts.num_peers;
  }

  stats.dht_enabled = session.session.is_dht_running();
  stats.dht_nodes = 0; // Would need session_status for this

  return stats;
}

DhtStats session_get_dht_stats(Session const &session) {
  DhtStats stats;
  stats.dht_nodes = 0;
  stats.dht_node_cache = 0;
  stats.dht_torrents = 0;
  stats.total_peers = 0;
  // Full DHT stats require more complex API
  return stats;
}

// ============================================================================
// DHT
// ============================================================================

bool session_is_dht_running(Session const &session) {
  return session.session.is_dht_running();
}

void session_add_dht_node(Session &session, rust::Str host, int32_t port) {
  session.session.add_dht_node({rust_str_to_std(host), port});
}

void session_dht_get_peers(Session &session, rust::Str info_hash) {
  lt::sha1_hash hash = hex_to_sha1(info_hash);
  session.session.dht_get_peers(hash);
}

// ============================================================================
// ALERTS
// ============================================================================

rust::Vec<AlertInfo> session_pop_alerts(Session &session) {
  rust::Vec<AlertInfo> result;
  std::vector<lt::alert *> alerts;
  session.session.pop_alerts(&alerts);

  for (const lt::alert *a : alerts) {
    AlertInfo info;
    info.alert_type = a->type();
    info.category = static_cast<int32_t>(static_cast<uint32_t>(a->category()));
    info.what = rust::String(a->what());
    info.message = rust::String::lossy(a->message());
    info.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                         a->timestamp().time_since_epoch())
                         .count();
    info.piece_index = -1; // Default: not a piece alert
    // piece_data is empty by default (rust::Vec default constructor)

    // Try to get info_hash if this is a torrent alert
    if (const auto *ta = dynamic_cast<const lt::torrent_alert *>(a)) {
      info.info_hash = sha1_to_hex(ta->handle.info_hashes().get_best());
    }

    // Extract piece index from piece_finished_alert
    if (const auto *pfa = dynamic_cast<const lt::piece_finished_alert *>(a)) {
      info.piece_index = static_cast<int32_t>(pfa->piece_index);
    }

    // Extract piece index from hash_failed_alert (to debug starved pieces)
    if (const auto *hfa = dynamic_cast<const lt::hash_failed_alert *>(a)) {
      info.piece_index = static_cast<int32_t>(hfa->piece_index);
    }

    // MEMORY-FIRST STREAMING: Extract piece data from read_piece_alert
    // This allows us to cache pieces in memory before they hit disk
    if (const auto *rpa = dynamic_cast<const lt::read_piece_alert *>(a)) {
      info.piece_index = static_cast<int32_t>(rpa->piece);
      if (rpa->buffer && rpa->size > 0) {
        info.piece_data.reserve(rpa->size);
        const char *buf = rpa->buffer.get();
        for (int i = 0; i < rpa->size; ++i) {
          info.piece_data.push_back(static_cast<uint8_t>(buf[i]));
        }
      }
    }

    result.push_back(std::move(info));
  }
  return result;
}

bool session_wait_for_alert(Session const &session, int32_t timeout_ms) {
  // wait_for_alert is actually non-const in libtorrent, but we cast away const
  // for this query
  return const_cast<lt::session &>(session.session)
      .wait_for_alert(std::chrono::milliseconds(timeout_ms));
}

void session_set_alert_mask(Session &session, uint32_t mask) {
  lt::settings_pack pack;
  pack.set_int(lt::settings_pack::alert_mask, static_cast<int>(mask));
  session.session.apply_settings(pack);
}

// ============================================================================
// STATE PERSISTENCE
// ============================================================================

rust::Vec<uint8_t> session_save_state(Session const &session) {
  // In ABI v3, use write_session_params instead of save_state
  auto params = session.session.session_state();
  std::vector<char> buf = lt::write_session_params_buf(params);
  rust::Vec<uint8_t> result;
  for (char c : buf)
    result.push_back(static_cast<uint8_t>(c));
  return result;
}

void session_load_state(Session &session, rust::Slice<const uint8_t> state) {
  // In ABI v3, use read_session_params instead of load_state
  lt::span<char const> span(reinterpret_cast<const char *>(state.data()),
                            state.size());
  lt::session_params params = lt::read_session_params(span);
  session.session.apply_settings(params.settings);
}

rust::Vec<uint8_t> session_save_dht_state(Session const &session) {
  // In ABI v3, use write_session_params with save_dht_state flag
  auto params = session.session.session_state(lt::session::save_dht_state);
  std::vector<char> buf =
      lt::write_session_params_buf(params, lt::session::save_dht_state);
  rust::Vec<uint8_t> result;
  for (char c : buf)
    result.push_back(static_cast<uint8_t>(c));
  return result;
}

void session_load_dht_state(Session &session,
                            rust::Slice<const uint8_t> state) {
  // In ABI v3, use read_session_params instead of load_state
  lt::span<char const> span(reinterpret_cast<const char *>(state.data()),
                            state.size());
  lt::session_params params =
      lt::read_session_params(span, lt::session::save_dht_state);
  // DHT state is part of session_params but can't be applied to running session
  // This would require recreating the session with these params
}

// ============================================================================
// TORRENT HANDLE - BASIC INFO
// ============================================================================

bool handle_is_valid(TorrentHandle const &handle) {
  return handle.handle.is_valid();
}

rust::String handle_get_info_hash(TorrentHandle const &handle) {
  return sha1_to_hex(handle.handle.info_hashes().get_best());
}

std::unique_ptr<TorrentHandle> handle_clone(TorrentHandle const &handle) {
  return std::make_unique<TorrentHandle>(handle.handle);
}

rust::String handle_get_info_hash_v2(TorrentHandle const &handle) {
  auto ih = handle.handle.info_hashes();
  return ih.has_v2() ? sha256_to_hex(ih.v2) : rust::String("");
}

rust::String handle_get_name(TorrentHandle const &handle) {
  return rust::String(
      handle.handle.status(lt::torrent_handle::query_name).name);
}

TorrentStatus handle_get_status(TorrentHandle const &handle) {
  return make_torrent_status(handle.handle.status(playback_status_flags()));
}

// ============================================================================
// TORRENT HANDLE - CONTROL
// ============================================================================

void handle_pause(TorrentHandle &handle) {
  // The session auto-manager resumes paused torrents that still carry the
  // auto_managed flag, so the flag must be cleared for the pause to stick.
  handle.handle.unset_flags(lt::torrent_flags::auto_managed);
  handle.handle.pause();
}

void handle_resume(TorrentHandle &handle) {
  // Keep the torrent MANUALLY managed while running. Re-arming auto_managed
  // here lets libtorrent's queue manager immediately re-pause a torrent it
  // considers a finished seed (e.g. only the already-watched files are wanted),
  // which races our explicit resume and leaves the torrent paused with zero
  // peers so a newly-requested file never starts downloading. With the flag
  // cleared the torrent stays running until we explicitly pause it again.
  handle.handle.unset_flags(lt::torrent_flags::auto_managed);
  handle.handle.resume();
}

void handle_set_upload_limit(TorrentHandle &handle, int32_t limit) {
  handle.handle.set_upload_limit(limit);
}

void handle_set_download_limit(TorrentHandle &handle, int32_t limit) {
  handle.handle.set_download_limit(limit);
}

int32_t handle_get_upload_limit(TorrentHandle const &handle) {
  return handle.handle.upload_limit();
}

int32_t handle_get_download_limit(TorrentHandle const &handle) {
  return handle.handle.download_limit();
}

void handle_force_recheck(TorrentHandle &handle) {
  handle.handle.force_recheck();
}

void handle_force_reannounce(TorrentHandle &handle) {
  handle.handle.force_reannounce();
}

void handle_force_reannounce_with_flags(TorrentHandle &handle,
                                        bool high_priority,
                                        bool ignore_min_interval) {
  auto flags = lt::reannounce_flags_t{};
#if LIBTORRENT_VERSION_NUM >= 20012
  if (high_priority)
    flags |= lt::torrent_handle::high_priority;
#else
  // high_priority was added in libtorrent 2.0.12.
  (void)high_priority;
#endif
  if (ignore_min_interval)
    flags |= lt::torrent_handle::ignore_min_interval;
  handle.handle.force_reannounce(0, -1, flags);
}

void handle_force_dht_announce(TorrentHandle &handle) {
  handle.handle.force_dht_announce();
}

void handle_flush_cache(TorrentHandle &handle) {
  handle.handle.flush_cache();
}

// ============================================================================
// TORRENT HANDLE - SEQUENTIAL/STREAMING
// ============================================================================

void handle_set_sequential_download(TorrentHandle &handle, bool enable) {
  if (enable)
    handle.handle.set_flags(lt::torrent_flags::sequential_download);
  else
    handle.handle.unset_flags(lt::torrent_flags::sequential_download);
}

bool handle_is_sequential_download(TorrentHandle const &handle) {
  return bool(handle.handle.flags() & lt::torrent_flags::sequential_download);
}

void handle_set_piece_deadline(TorrentHandle &handle, int32_t piece,
                               int32_t deadline_ms) {
  handle.handle.set_piece_deadline(lt::piece_index_t(piece), deadline_ms);
}

void handle_set_piece_deadline_with_alert(TorrentHandle &handle, int32_t piece,
                                          int32_t deadline_ms,
                                          bool alert_when_available) {
  if (alert_when_available) {
    handle.handle.set_piece_deadline(lt::piece_index_t(piece), deadline_ms,
                                     lt::torrent_handle::alert_when_available);
  } else {
    handle.handle.set_piece_deadline(lt::piece_index_t(piece), deadline_ms);
  }
}

void handle_reset_piece_deadline(TorrentHandle &handle, int32_t piece) {
  handle.handle.reset_piece_deadline(lt::piece_index_t(piece));
}

void handle_clear_piece_deadlines(TorrentHandle &handle) {
  handle.handle.clear_piece_deadlines();
}

// ============================================================================
// TORRENT HANDLE - FILES
// ============================================================================

rust::Vec<FileInfo> handle_get_files(TorrentHandle const &handle) {
  rust::Vec<FileInfo> result;

  auto ti = handle.handle.torrent_file();
  if (!ti)
    return result;

  auto ts =
      handle.handle.status(lt::torrent_handle::query_save_path);
  const lt::file_storage &files = ti->layout();
  auto file_progress = handle.handle.file_progress();
  auto priorities = handle.handle.get_file_priorities();

  for (lt::file_index_t i(0); i < files.end_file(); ++i) {
    FileInfo info;
    info.index = static_cast<int32_t>(static_cast<int>(i));
    info.path = rust::String(files.file_path(i));
    info.absolute_path = rust::String(ts.save_path + "/" + files.file_path(i));
    info.size = files.file_size(i);
    info.downloaded =
        file_progress.size() > static_cast<std::size_t>(static_cast<int>(i))
            ? file_progress[static_cast<int>(i)]
            : 0;
    info.priority =
        priorities.size() > static_cast<std::size_t>(static_cast<int>(i))
            ? static_cast<int32_t>(
                  static_cast<uint8_t>(priorities[static_cast<int>(i)]))
            : 4;
    info.progress = info.size > 0 ? static_cast<float>(info.downloaded) /
                                        static_cast<float>(info.size)
                                  : 0.0f;

    // Calculate first and last piece from file offset and size
    auto file_offset = files.file_offset(i);
    auto file_size = files.file_size(i);
    auto piece_len = ti->piece_length();
    info.first_piece = static_cast<int32_t>(file_offset / piece_len);
    info.last_piece =
        static_cast<int32_t>((file_offset + file_size - 1) / piece_len);
    info.offset = file_offset;

    result.push_back(std::move(info));
  }
  return result;
}

rust::Vec<int32_t> handle_get_file_priorities(TorrentHandle const &handle) {
  rust::Vec<int32_t> result;
  for (auto p : handle.handle.get_file_priorities())
    result.push_back(static_cast<int32_t>(static_cast<uint8_t>(p)));
  return result;
}

void handle_set_file_priority(TorrentHandle &handle, int32_t index,
                              int32_t priority) {
  handle.handle.file_priority(
      lt::file_index_t(index),
      lt::download_priority_t(static_cast<uint8_t>(priority)));
}

void handle_set_file_priorities(TorrentHandle &handle,
                                rust::Slice<const int32_t> priorities) {
  std::vector<lt::download_priority_t> prios;
  for (int32_t p : priorities)
    prios.push_back(lt::download_priority_t(static_cast<uint8_t>(p)));
  handle.handle.prioritize_files(prios);
}

void handle_rename_file(TorrentHandle &handle, int32_t index,
                        rust::Str new_name) {
  handle.handle.rename_file(lt::file_index_t(index), rust_str_to_std(new_name));
}

void handle_move_storage(TorrentHandle &handle, rust::Str new_path) {
  handle.handle.move_storage(rust_str_to_std(new_path));
}

// ============================================================================
// TORRENT HANDLE - PIECES
// ============================================================================

int32_t handle_num_pieces(TorrentHandle const &handle) {
  auto ti = handle.handle.torrent_file();
  return ti ? ti->num_pieces() : 0;
}

int32_t handle_piece_length(TorrentHandle const &handle) {
  auto ti = handle.handle.torrent_file();
  return ti ? ti->piece_length() : 0;
}

bool handle_have_piece(TorrentHandle const &handle, int32_t piece) {
  return handle.handle.have_piece(lt::piece_index_t(piece));
}

rust::Vec<uint8_t> handle_get_piece_presence(TorrentHandle const &handle,
                                             int32_t first, int32_t last) {
  rust::Vec<uint8_t> result;
  auto const ti = handle.handle.torrent_file();
  if (!ti || first < 0 || last < first || first >= ti->num_pieces())
    return result;

  last = std::min(last, ti->num_pieces() - 1);
  auto const status =
      handle.handle.status(lt::torrent_handle::query_pieces);
  result.reserve(static_cast<std::size_t>(last - first + 1));
  for (int32_t piece = first; piece <= last; ++piece) {
    bool const present = status.is_seeding ||
                         (!status.pieces.empty() &&
                          status.pieces[lt::piece_index_t(piece)]);
    result.push_back(present ? uint8_t{1} : uint8_t{0});
  }
  return result;
}

rust::Vec<int32_t> handle_get_piece_availability(TorrentHandle const &handle) {
  rust::Vec<int32_t> result;
  std::vector<int> avail;
  handle.handle.piece_availability(avail);
  for (int a : avail)
    result.push_back(a);
  return result;
}

void handle_set_piece_priority(TorrentHandle &handle, int32_t piece,
                               int32_t priority) {
  handle.handle.piece_priority(
      lt::piece_index_t(piece),
      lt::download_priority_t(static_cast<uint8_t>(priority)));
}

void handle_set_piece_priorities(
    TorrentHandle &handle, rust::Slice<const PiecePriority> priorities) {
  std::vector<std::pair<lt::piece_index_t, lt::download_priority_t>> updates;
  updates.reserve(priorities.size());
  for (auto const &priority : priorities) {
    updates.emplace_back(
        lt::piece_index_t(priority.piece),
        lt::download_priority_t(static_cast<uint8_t>(priority.priority)));
  }
  if (!updates.empty())
    handle.handle.prioritize_pieces(updates);
}

rust::Vec<int32_t> handle_get_piece_priorities(TorrentHandle const &handle) {
  rust::Vec<int32_t> result;
  for (auto p : handle.handle.get_piece_priorities())
    result.push_back(static_cast<int32_t>(static_cast<uint8_t>(p)));
  return result;
}

void handle_read_piece(TorrentHandle &handle, int32_t piece) {
  handle.handle.read_piece(lt::piece_index_t(piece));
}

// ============================================================================
// TORRENT HANDLE - PEERS
// ============================================================================

rust::Vec<PeerInfo> handle_get_peers(TorrentHandle const &handle) {
  rust::Vec<PeerInfo> result;
  std::vector<lt::peer_info> peers;
  handle.handle.get_peer_info(peers);

  for (const auto &p : peers) {
    PeerInfo info;
    auto const endpoint = p.remote_endpoint();
    info.ip = rust::String(endpoint.address().to_string());
    info.port = endpoint.port();
    info.client = rust::String(p.client);
    info.download_rate = p.down_speed;
    info.upload_rate = p.up_speed;
    info.total_download = p.total_download;
    info.total_upload = p.total_upload;
    info.progress = p.progress;
    info.is_seed = bool(p.flags & lt::peer_info::seed);
    info.is_connecting = bool(p.flags & lt::peer_info::connecting);
    info.is_handshake = bool(p.flags & lt::peer_info::handshake);
    // TODO: Fix connection_type cast for ABI v2 - using 0 as placeholder
    info.connection_type = 0;
    info.country = rust::String(""); // country removed in libtorrent 2.0
    result.push_back(std::move(info));
  }
  return result;
}

void handle_connect_peer(TorrentHandle &handle, rust::Str ip, uint16_t port) {
  lt::error_code ec;
  auto addr = lt::make_address(rust_str_to_std(ip), ec);
  if (ec)
    throw std::runtime_error("Invalid IP address");
  handle.handle.connect_peer({addr, port});
}

// ============================================================================
// TORRENT HANDLE - TRACKERS
// ============================================================================

rust::Vec<TrackerInfo> handle_get_trackers(TorrentHandle const &handle) {
  rust::Vec<TrackerInfo> result;
  for (const auto &ae : handle.handle.trackers()) {
    TrackerInfo info;
    info.url = rust::String(ae.url);
    info.tier = static_cast<int32_t>(ae.tier);
    info.status = 1; // enabled
    info.message = rust::String("");
    info.num_peers = 0;
    info.scrape_incomplete = -1;
    info.scrape_complete = -1;
    info.next_announce_seconds = 0;

    // Get info from endpoints if available
    for (const auto &ep : ae.endpoints) {
      // In libtorrent 2.0, info_hashes is indexed by protocol_version
      const auto &ti = ep.info_hashes[lt::protocol_version::V1];
      if (ti.scrape_complete >= 0 || ti.scrape_incomplete >= 0) {
        info.num_peers = ti.scrape_incomplete + ti.scrape_complete;
        info.scrape_incomplete = ti.scrape_incomplete;
        info.scrape_complete = ti.scrape_complete;
        info.message = rust::String(ti.message);
        break;
      }
    }

    result.push_back(std::move(info));
  }
  return result;
}

void handle_add_tracker(TorrentHandle &handle, rust::Str url, int32_t tier) {
  lt::announce_entry ae(rust_str_to_std(url));
  ae.tier = static_cast<uint8_t>(tier);
  handle.handle.add_tracker(ae);
}

void handle_remove_tracker(TorrentHandle &handle, rust::Str url) {
  auto trackers = handle.handle.trackers();
  trackers.erase(std::remove_if(trackers.begin(), trackers.end(),
                                [&](const lt::announce_entry &ae) {
                                  return ae.url == rust_str_to_std(url);
                                }),
                 trackers.end());
  handle.handle.replace_trackers(trackers);
}

void handle_replace_trackers(TorrentHandle &handle,
                             rust::Slice<const rust::String> urls) {
  std::vector<lt::announce_entry> trackers;
  for (const auto &url : urls)
    trackers.emplace_back(std::string(url.data(), url.size()));
  handle.handle.replace_trackers(trackers);
}

// ============================================================================
// TORRENT HANDLE - SAVE/RESUME
// ============================================================================

void handle_save_resume_data(TorrentHandle &handle) {
  handle.handle.save_resume_data();
}

bool handle_need_save_resume_data(TorrentHandle const &handle) {
  return handle.handle.need_save_resume_data();
}

// ============================================================================
// UTILITIES
// ============================================================================

AddTorrentParams parse_magnet_uri(rust::Str uri) {
  lt::add_torrent_params p;
  lt::error_code ec;
  lt::parse_magnet_uri(rust_str_to_std(uri), p, ec);
  if (ec)
    throw std::runtime_error("Failed to parse magnet: " + ec.message());

  AddTorrentParams result;
  result.magnet_uri = rust::String(rust_str_to_std(uri));
  result.name = rust::String(p.name);
  for (const auto &t : p.trackers)
    result.trackers.push_back(rust::String(t));
  return result;
}

rust::Vec<uint8_t> handle_get_metadata(TorrentHandle const &handle) {
  rust::Vec<uint8_t> result;
  if (!handle.handle.torrent_file())
    return result;

  auto const params =
      handle.handle.get_resume_data(lt::torrent_handle::save_info_dict);
  auto const buf = lt::write_torrent_file_buf(
      params, lt::write_flags::allow_missing_piece_layer);
  for (char c : buf)
    result.push_back(static_cast<uint8_t>(c));
  return result;
}

rust::String make_magnet_uri(TorrentHandle const &handle) {
  if (!handle.handle.is_valid())
    return rust::String();

  auto const params = handle.handle.get_resume_data();
  return rust::String(lt::make_magnet_uri(params));
}

rust::String libtorrent_version() { return rust::String(lt::version()); }

int32_t get_piece_finished_alert_type() {
  return lt::piece_finished_alert::alert_type;
}

int32_t get_read_piece_alert_type() { return lt::read_piece_alert::alert_type; }

int32_t get_metadata_received_alert_type() {
  return lt::metadata_received_alert::alert_type;
}

int32_t get_hash_failed_alert_type() {
  return lt::hash_failed_alert::alert_type;
}

int32_t get_file_prio_alert_type() {
  return lt::file_prio_alert::alert_type;
}

int32_t get_torrent_paused_alert_type() {
  return lt::torrent_paused_alert::alert_type;
}

int32_t get_torrent_resumed_alert_type() {
  return lt::torrent_resumed_alert::alert_type;
}

int32_t get_file_error_alert_type() {
  return lt::file_error_alert::alert_type;
}

int32_t get_performance_alert_type() {
  return lt::performance_alert::alert_type;
}

} // namespace libtorrent_wrapper
