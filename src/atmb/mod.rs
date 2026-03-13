use color_eyre::eyre::{bail, eyre};
use futures::StreamExt;
use log::info;
use reqwest::Client;
use rand::Rng;
use std::collections::HashSet;
use std::fs;
use tokio::time::{sleep, Duration};
use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT};
use crate::atmb::model::Mailbox;
use crate::atmb::page::{CountryPage, LocationDetailPage, StatePage};
use crate::utils::retry_wrapper;


mod page;
pub mod model;

const BASE_URL: &str = "https://www.anytimemailbox.com";
const UA: &str = "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0";

const US_HOME_PAGE_URL: &str = "/l/usa";

const CACHE_FILE: &str = "mailbox_cache.json";


/// HTTP client for obtaining information from ATMB
struct ATMBClient {
    client: Client,
}

impl ATMBClient {
    fn new() -> color_eyre::Result<Self> {
        Ok(
            Self {
                client: Client::builder()
                    .default_headers(Self::default_headers())
                    .build()?,
            }
        )
    }

    fn default_headers() -> HeaderMap {
        let mut map = HeaderMap::new();
        map.insert(USER_AGENT, HeaderValue::from_static(UA));
        map
    }

    /// get the content of a page
    ///
    /// * `url_path` - the path of the page, can be either a full URL or a relative path
    async fn fetch_page(&self, url_path: &str) -> color_eyre::Result<String> {
        let url = if url_path.starts_with("http") {
            url_path
        } else {
            &format!("{}{}", BASE_URL, url_path)
        };
        Ok(
            retry_wrapper(3, || async {
                self.client
                    .get(url)
                    .send()
                    .await?
                    .text()
                    .await
            }).await?
        )
    }
}

pub struct ATMBCrawl {
    client: ATMBClient,
}

impl ATMBCrawl {
    pub fn new() -> color_eyre::Result<Self> {
        Ok(
            Self {
                client: ATMBClient::new()?,
            }
        )
    }

    pub async fn fetch(&self) -> color_eyre::Result<Vec<Mailbox>> {
        // we're only interested in US, so hardcode here.
        let country_html = self.client.fetch_page(US_HOME_PAGE_URL).await?;
        let country_page = CountryPage::parse_html(&country_html)?;

        let state_pages = self.fetch_state_pages(&country_page).await?;
        let total_num = state_pages.iter().map(|sp| sp.len()).sum::<usize>();

        let mailboxes = state_pages.into_iter()
            .filter_map(|sp| match sp.to_mailboxes() {
                Ok(mailboxes) => Some(mailboxes),
                Err(e) => {
                    log::error!("cannot convert state page to mailboxes: {:?}", e);
                    None
                }
            })
            .flatten()
            .collect::<Vec<_>>();

        if mailboxes.len() != total_num {
            bail!("Some mailboxes cannot be fetched");
        }

        // visit every mailbox detail page to get the address line 2
        self.update_street2_for_mailbox(mailboxes).await.map_err(|e| {
            eyre!("Some mailbox's detail cannot be fetched: {:?}", e)
        })
    }

    // 确保你在文件开头引入了 tokio 的 sleep，而不是 std::thread::sleep
    // use tokio::time::{sleep, Duration};

    // async fn update_street2_for_mailbox(&self, mailboxes: Vec<Mailbox>) -> color_eyre::Result<Vec<Mailbox>> {
    //     let total_mailboxes = mailboxes.len();
    //
    //     let mailboxes = futures::stream::iter(mailboxes).enumerate().map(|(idx, mut mailbox)| {
    //         let link = mailbox.link.clone();
    //         async move {
    //             let fut = || async {
    //                 info!("[{}/{}] fetching the detail page of [{}]...", idx + 1, total_mailboxes, mailbox.name);
    //                 let mut rng = rand::thread_rng();
    //
    //                 // 🌟 稍微拉长一点延时，伪装得更像人类一点
    //                 let delay_ms = rng.gen_range(2000..4500);
    //
    //                 println!("等待 {} 毫秒以防被封...", delay_ms);
    //
    //                 // 🌟 修复 1：必须加上 .await！
    //                 tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
    //
    //                 let detail_page = self.fetch_location_detail_page(&mailbox.link).await?;
    //                 mailbox.address.line1 = detail_page.street();
    //                 Result::<_, color_eyre::eyre::Error>::Ok(mailbox)
    //             };
    //             fut().await
    //                 .map_err(|err| {
    //                     let err = eyre!("cannot fetch detail page for: [{}]: {:?}", link, err);
    //                     log::error!("{:?}", err);
    //                     err
    //                 })
    //         }
    //     })
    //         // 🌟 修复 2：降低并发数！先改成 1 或 2 测试，成功了再慢慢往上调
    //         .buffer_unordered(2)
    //         .collect::<Vec<_>>()
    //         .await;
    //
    //     let (suc_list, err_list): (Vec<_>, Vec<_>) = mailboxes.into_iter().partition(Result::is_ok);
    //     let suc_list = suc_list.into_iter().filter_map(Result::ok).collect::<Vec<_>>();
    //     let err_list = err_list.into_iter().filter_map(Result::err).collect::<Vec<_>>();
    //
    //     if !err_list.is_empty() {
    //         bail!("{:#?}", err_list);
    //     } else {
    //         Ok(suc_list)
    //     }
    // }

    // 定义一个缓存文件的路径

    async fn update_street2_for_mailbox(&self, mailboxes: Vec<Mailbox>) -> color_eyre::Result<Vec<Mailbox>> {
        // 1. 读取本地缓存（断点续传）
        let mut completed_mailboxes: Vec<Mailbox> = if let Ok(data) = fs::read_to_string(CACHE_FILE) {
            serde_json::from_str(&data).unwrap_or_else(|_| vec![])
        } else {
            vec![]
        };

        // 提取已完成的链接，用于快速过滤
        let completed_links: HashSet<String> = completed_mailboxes.iter().map(|m| m.link.clone()).collect();

        // 2. 过滤出还没有抓取过的 mailbox（使用 mut 以便后续转移所有权）
        let mut pending_mailboxes: Vec<Mailbox> = mailboxes
            .into_iter()
            .filter(|m| !completed_links.contains(&m.link))
            .collect();

        let total_pending = pending_mailboxes.len();
        if total_pending == 0 {
            log::info!("✅ 所有页面都已抓取完毕，直接返回缓存数据！");
            return Ok(completed_mailboxes);
        }

        log::info!("🔍 发现 {} 个已完成记录，还有 {} 个待抓取...", completed_links.len(), total_pending);

        // 3. 核心抓取循环：加入【熔断机制】和【实时保存】
        let mut consecutive_errors = 0;
        const MAX_CONSECUTIVE_ERRORS: usize = 10;
        let mut total_processed = 0;

        // 使用 while + drain 转移所有权，完美避开生命周期和借用报错
        while !pending_mailboxes.is_empty() {
            // 每次最多取 2 个发起并发请求，防止并发过高被封
            let chunk_size = pending_mailboxes.len().min(2);
            let chunk: Vec<Mailbox> = pending_mailboxes.drain(..chunk_size).collect();

            // 构造并发任务
            let chunk_futures = chunk.into_iter().enumerate().map(|(i, mut mb)| {
                let current_idx = total_processed + i + 1;

                async move {
                    log::info!("[{}/{}] fetching the detail page of [{}]...", current_idx, total_pending, mb.name);

                    // 随机延时防封 (2秒到4.5秒之间)
                    let mut rng = rand::thread_rng();
                    let delay_ms = rng.gen_range(2000..4500);
                    sleep(Duration::from_millis(delay_ms)).await;

                    // 执行真正的网络请求与 HTML 解析
                    let res = self.fetch_location_detail_page(&mb.link).await;
                    (mb, res)
                }
            });

            // 并发等待这批任务完成
            let results = futures::future::join_all(chunk_futures).await;
            total_processed += chunk_size;

            let mut has_new_data = false;

            // 检查结果并更新缓存数组
            for (mut mb, res) in results {
                match res {
                    Ok(detail_page) => {
                        mb.address.line1 = detail_page.street();
                        completed_mailboxes.push(mb);
                        consecutive_errors = 0; // 只要成功一次，连续错误立即清零
                        has_new_data = true;
                    }
                    Err(err) => {
                        log::error!("❌ 抓取失败 [{}]: {:?}", mb.link, err);
                        consecutive_errors += 1; // 失败累加
                    }
                }
            }

            // 🌟 实时保存机制：每抓完一小批，只要有新数据就落盘
            if has_new_data {
                if let Ok(json_str) = serde_json::to_string_pretty(&completed_mailboxes) {
                    if let Err(e) = fs::write(CACHE_FILE, json_str) {
                        log::error!("⚠️ 写入缓存文件失败: {:?}", e);
                    }
                }
            }

            // 🌟 熔断判断：连续错误达到上限，直接跳出循环
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                log::error!("🚨 触发熔断保护：连续失败 {} 次！疑似被封禁或页面结构大改，停止抓取。", consecutive_errors);
                break;
            }
        }

        // 4. 结束处理：判断是因为全部抓完退出的，还是因为熔断强行中断的
        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
            bail!("⛔ 抓取被强行中止：连续错误达到 {} 次。当前进度已安全保存至 {}，你可以稍后换个网络（或代理）再试。", MAX_CONSECUTIVE_ERRORS, CACHE_FILE);
        }

        Ok(completed_mailboxes)
    }
    async fn fetch_state_pages(&self, country_page: &CountryPage<'_>) -> color_eyre::Result<Vec<StatePage>> {
        let total_states = country_page.states.len();
        let state_pages: Vec<color_eyre::Result<StatePage>> = futures::stream::iter(&country_page.states).enumerate().map(|(idx, state_html_info)| {
            info!("[{}/{total_states}] fetching [{}] state page...", idx + 1, state_html_info.name());
            async move {
                let state_html = self.client.fetch_page(state_html_info.url()).await?;
                Ok(StatePage::parse_html(&state_html)?)
            }
        })
            // limit concurrent requests to 5
            .buffer_unordered(5)
            .collect()
            .await;

        if state_pages.iter().filter_map(|state_page| match state_page {
            Err(e) => {
                log::error!("cannot fetch state: {:?}", e);
                Some(())
            }
            _ => None
        })
            .count() != 0 {
            bail!("Some states cannot be fetched");
        }
        Ok(state_pages.into_iter().map(|state_page| state_page.unwrap()).collect())
    }

    async fn fetch_location_detail_page(&self, mailbox_link: &str) -> color_eyre::Result<LocationDetailPage> {
        let html = self.client.fetch_page(mailbox_link).await?;
        Ok(LocationDetailPage::parse_html(&html)?)
    }
}