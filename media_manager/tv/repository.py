from typing import TypedDict, Unpack, overload

from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session, joinedload

from media_manager.exceptions import (
    EpisodeNotFoundError,
    ExternalShowNotFoundError,
    SeasonNotFoundError,
    SeasonWithinShowNotFoundError,
    ShowNotFoundError,
)
from media_manager.torrent.models import Torrent
from media_manager.torrent.schemas import Torrent as TorrentSchema
from media_manager.torrent.schemas import TorrentId
from media_manager.tv.models import Episode, EpisodeFile, Season, Show
from media_manager.tv.schemas import Episode as EpisodeSchema
from media_manager.tv.schemas import EpisodeFile as EpisodeFileSchema
from media_manager.tv.schemas import (
    EpisodeId,
    EpisodeNumber,
    SeasonId,
    SeasonNumber,
    ShowId,
)
from media_manager.tv.schemas import Season as SeasonSchema
from media_manager.tv.schemas import Show as ShowSchema


class ShowAttributes(TypedDict, total=False):
    name: str
    overview: str
    year: int | None
    ended: bool
    continuous_download: bool
    imdb_id: str | None


class SeasonAttributes(TypedDict, total=False):
    name: str
    overview: str


class EpisodeAttributes(TypedDict, total=False):
    title: str
    overview: str | None


@overload
def _update_db_fields(db_object: Show, fields: ShowAttributes) -> bool: ...
@overload
def _update_db_fields(db_object: Season, fields: SeasonAttributes) -> bool: ...
@overload
def _update_db_fields(db_object: Episode, fields: EpisodeAttributes) -> bool: ...


def _update_db_fields(
    db_object: Show | Season | Episode,
    fields: ShowAttributes | SeasonAttributes | EpisodeAttributes,
) -> bool:
    """
    Updates the field of `db_object` with the values from `fields` if they
    are different from the value in the `db_object`.
    Returns True if any field was updated, False otherwise.
    """
    updated = False
    for attr, value in fields.items():
        if getattr(db_object, attr) != value:
            setattr(db_object, attr, value)
            updated = True
    return updated


class TvRepository:
    """
    Repository for managing TV shows, seasons, and episodes in the database.
    Provides methods to retrieve, save, and delete shows and seasons.
    """

    def __init__(self, db: Session) -> None:
        self.db = db

    def get_show_by_id(self, show_id: ShowId) -> ShowSchema:
        """
        Retrieve a show by its ID, including seasons and episodes.

        :param show_id: The ID of the show to retrieve.
        :return: A ShowSchema object if found.
        :raises ShowNotFoundError: If the show with the given ID is not found.
        """
        stmt = (
            select(Show)
            .where(Show.id == show_id)
            .options(joinedload(Show.seasons).joinedload(Season.episodes))
        )
        result = self.db.execute(stmt).unique().scalar_one_or_none()
        if not result:
            raise ShowNotFoundError(show_id)
        return ShowSchema.model_validate(result)

    def get_show_by_external_id(
        self, external_id: int, metadata_provider: str
    ) -> ShowSchema:
        """
        Retrieve a show by its external ID, including nested seasons and episodes.

        :param external_id: The ID of the show to retrieve.
        :param metadata_provider: The metadata provider associated with the ID.
        :return: A ShowSchema object if found.
        :raises ExternalShowNotFoundError: If the show with the given external ID and provider is not found.
        """
        stmt = (
            select(Show)
            .where(Show.external_id == external_id)
            .where(Show.metadata_provider == metadata_provider)
            .options(joinedload(Show.seasons).joinedload(Season.episodes))
        )
        result = self.db.execute(stmt).unique().scalar_one_or_none()
        if not result:
            raise ExternalShowNotFoundError(external_id, metadata_provider)
        return ShowSchema.model_validate(result)

    def get_shows(self) -> list[ShowSchema]:
        """
        Retrieve all shows from the database.

        :return: A list of ShowSchema objects.
        """
        stmt = select(Show).options(
            joinedload(Show.seasons).joinedload(Season.episodes)
        )
        results = self.db.execute(stmt).scalars().unique().all()
        return [ShowSchema.model_validate(show) for show in results]

    def get_total_downloaded_episodes_count(self) -> int:
        """
        Count downloaded episodes in the database.

        :return: The total number of downloaded episodes.
        """
        stmt = select(func.count(Episode.id)).select_from(Episode).join(EpisodeFile)
        return self.db.execute(stmt).scalar_one_or_none() or 0

    def save_show(self, show: ShowSchema) -> ShowSchema:
        """
        Save a new show or update an existing one in the database.

        :param show: The ShowSchema object to save.
        :return: The saved ShowSchema object.
        """
        db_show = self.db.get(Show, show.id) if show.id else None

        if db_show:  # Update existing show
            db_show.external_id = show.external_id
            db_show.metadata_provider = show.metadata_provider
            db_show.name = show.name
            db_show.overview = show.overview
            db_show.year = show.year
            db_show.original_language = show.original_language
            db_show.imdb_id = show.imdb_id
        else:  # Insert new show
            db_show = Show(
                id=show.id,
                external_id=show.external_id,
                metadata_provider=show.metadata_provider,
                name=show.name,
                overview=show.overview,
                year=show.year,
                ended=show.ended,
                original_language=show.original_language,
                imdb_id=show.imdb_id,
                seasons=[
                    Season(
                        id=season.id,
                        show_id=show.id,
                        number=season.number,
                        external_id=season.external_id,
                        name=season.name,
                        overview=season.overview,
                        episodes=[
                            Episode(
                                id=episode.id,
                                season_id=season.id,
                                number=episode.number,
                                external_id=episode.external_id,
                                title=episode.title,
                                overview=episode.overview,
                            )
                            for episode in season.episodes
                        ],
                    )
                    for season in show.seasons
                ],
            )
            self.db.add(db_show)

        self.db.commit()
        self.db.refresh(db_show)
        return ShowSchema.model_validate(db_show)

    def delete_show(self, show_id: ShowId) -> None:
        """
        Delete a show by its ID.

        :param show_id: The ID of the show to delete.
        :raises ShowNotFoundError: If the show with the given ID is not found.
        """
        with self.db.begin():
            show = self.db.get(Show, show_id)
            if not show:
                raise ShowNotFoundError(show_id)
            self.db.delete(show)

    def get_season(self, season_id: SeasonId) -> SeasonSchema:
        """
        Retrieve a season by its ID.

        :param season_id: The ID of the season to get.
        :return: A SeasonSchema object.
        :raises SeasonNotFoundError: If the season with the given ID is not found.
        """
        season = self.db.get(Season, season_id)
        if not season:
            raise SeasonNotFoundError(season_id)
        return SeasonSchema.model_validate(season)

    def get_episode(self, episode_id: EpisodeId) -> EpisodeSchema:
        """
        Retrieve an episode by its ID.

        :param episode_id: The ID of the episode to get.
        :return: An EpisodeSchema object.
        :raises EpisodeNotFoundError: If the episode with the given ID is not found.
        """
        episode = self.db.get(Episode, episode_id)
        if not episode:
            raise EpisodeNotFoundError(episode_id)
        return EpisodeSchema.model_validate(episode)

    def get_season_by_episode(self, episode_id: EpisodeId) -> SeasonSchema:
        """
        Retrieve the season for a given episode.

        :param episode_id: The ID of the episode.
        :return: A SeasonSchema object.
        :raises EpisodeNotFoundError: If no season is found for the episode.
        """
        stmt = select(Season).join(Season.episodes).where(Episode.id == episode_id)
        season = self.db.scalar(stmt)
        if not season:
            raise EpisodeNotFoundError(episode_id)

        return SeasonSchema.model_validate(season)

    def get_season_by_number(self, season_number: int, show_id: ShowId) -> SeasonSchema:
        """
        Retrieve a season by its number and show ID.

        :param season_number: The number of the season.
        :param show_id: The ID of the show.
        :return: A SeasonSchema object.
        :raises SeasonWithinShowNotFoundError: If the season is not found.
        """
        stmt = (
            select(Season)
            .where(Season.show_id == show_id)
            .where(Season.number == season_number)
            .options(joinedload(Season.episodes), joinedload(Season.show))
        )
        result = self.db.execute(stmt).unique().scalar_one_or_none()
        if not result:
            raise SeasonWithinShowNotFoundError(season_number, show_id)
        return SeasonSchema.model_validate(result)

    def add_episode_file(self, episode_file: EpisodeFileSchema) -> EpisodeFileSchema:
        """
        Adds an episode file record to the database.

        :param episode_file: The EpisodeFileSchema object to add.
        :return: The added EpisodeFileSchema object.
        :raises IntegrityError: If the record violates constraints.
        """
        db_model = EpisodeFile(**episode_file.model_dump())
        with self.db.begin():
            self.db.add(db_model)
        self.db.refresh(db_model)
        return EpisodeFileSchema.model_validate(db_model)

    def remove_episode_files_by_torrent_id(self, torrent_id: TorrentId) -> int:
        """
        Removes episode file records associated with a given torrent ID.

        :param torrent_id: The ID of the torrent whose episode files are to be removed.
        :return: The number of episode files removed.
        """
        with self.db.begin():
            stmt = delete(EpisodeFile).where(EpisodeFile.torrent_id == torrent_id)
            result = self.db.execute(stmt)
        return result.rowcount

    def set_show_library(self, show_id: ShowId, library: str) -> None:
        """
        Sets the library for a show.

        :param show_id: The ID of the show to update.
        :param library: The library path to set for the show.
        :raises ShowNotFoundError: If the show with the given ID is not found.
        """
        with self.db.begin():
            show = self.db.get(Show, show_id)
            if not show:
                raise ShowNotFoundError(show_id)
            show.library = library

    def get_episode_files_by_season_id(
        self, season_id: SeasonId
    ) -> list[EpisodeFileSchema]:
        """
        Retrieve all episode files for a given season ID.

        :param season_id: The ID of the season.
        :return: A list of EpisodeFileSchema objects.
        """
        stmt = select(EpisodeFile).join(Episode).where(Episode.season_id == season_id)
        results = self.db.execute(stmt).scalars().all()
        return [EpisodeFileSchema.model_validate(ef) for ef in results]

    def get_episode_files_by_episode_id(
        self, episode_id: EpisodeId
    ) -> list[EpisodeFileSchema]:
        """
        Retrieve all episode files for a given episode ID.

        :param episode_id: The ID of the episode.
        :return: A list of EpisodeFileSchema objects.
        """
        stmt = select(EpisodeFile).where(EpisodeFile.episode_id == episode_id)
        results = self.db.execute(stmt).scalars().all()
        return [EpisodeFileSchema.model_validate(sf) for sf in results]

    def get_torrents_by_show_id(self, show_id: ShowId) -> list[TorrentSchema]:
        """
        Retrieve all torrents associated with a given show ID.

        :param show_id: The ID of the show.
        :return: A list of TorrentSchema objects.
        """
        stmt = (
            select(Torrent)
            .distinct()
            .join(EpisodeFile, EpisodeFile.torrent_id == Torrent.id)
            .join(Episode, Episode.id == EpisodeFile.episode_id)
            .join(Season, Season.id == Episode.season_id)
            .where(Season.show_id == show_id)
        )
        results = self.db.execute(stmt).scalars().unique().all()
        return [TorrentSchema.model_validate(torrent) for torrent in results]

    def get_all_shows_with_torrents(self) -> list[ShowSchema]:
        """
        Retrieve all shows that are associated with a torrent, ordered alphabetically by show name.

        :return: A list of ShowSchema objects.
        """
        stmt = (
            select(Show)
            .distinct()
            .join(Season, Show.id == Season.show_id)
            .join(Episode, Season.id == Episode.season_id)
            .join(EpisodeFile, Episode.id == EpisodeFile.episode_id)
            .join(Torrent, EpisodeFile.torrent_id == Torrent.id)
            .options(joinedload(Show.seasons).joinedload(Season.episodes))
            .order_by(Show.name)
        )
        results = self.db.execute(stmt).scalars().unique().all()
        return [ShowSchema.model_validate(show) for show in results]

    def get_seasons_by_torrent_id(self, torrent_id: TorrentId) -> list[SeasonNumber]:
        """
        Retrieve season numbers associated with a given torrent ID.

        :param torrent_id: The ID of the torrent.
        :return: A list of SeasonNumber objects.
        """
        stmt = (
            select(Season.number)
            .distinct()
            .join(Episode, Episode.season_id == Season.id)
            .join(EpisodeFile, EpisodeFile.episode_id == Episode.id)
            .where(EpisodeFile.torrent_id == torrent_id)
        )
        results = self.db.execute(stmt).scalars().unique().all()
        return [SeasonNumber(x) for x in results]

    def get_episodes_by_torrent_id(self, torrent_id: TorrentId) -> list[EpisodeNumber]:
        """
        Retrieve episode numbers associated with a given torrent ID.

        :param torrent_id: The ID of the torrent.
        :return: A list of EpisodeNumber objects.
        """
        stmt = (
            select(Episode.number)
            .join(EpisodeFile, EpisodeFile.episode_id == Episode.id)
            .where(EpisodeFile.torrent_id == torrent_id)
            .order_by(Episode.number)
        )

        episode_numbers = self.db.execute(stmt).scalars().all()

        return [EpisodeNumber(n) for n in sorted(set(episode_numbers))]

    def get_show_by_season_id(self, season_id: SeasonId) -> ShowSchema:
        """
        Retrieve a show by one of its season's ID.

        :param season_id: The ID of the season to retrieve the show for.
        :return: A ShowSchema object.
        :raises SeasonNotFoundError: If the show for the given season ID is not found.
        """
        stmt = (
            select(Show)
            .join(Season, Show.id == Season.show_id)
            .where(Season.id == season_id)
            .options(joinedload(Show.seasons).joinedload(Season.episodes))
        )
        result = self.db.execute(stmt).unique().scalar_one_or_none()
        if not result:
            raise SeasonNotFoundError(season_id)
        return ShowSchema.model_validate(result)

    def add_season_to_show(
        self, show_id: ShowId, season_data: SeasonSchema
    ) -> SeasonSchema:
        """
        Adds a new season and its episodes to a show.
        If the season number already exists for the show, it returns the existing season.

        :param show_id: The ID of the show to add the season to.
        :param season_data: The SeasonSchema object for the new season.
        :return: The added or existing SeasonSchema object.
        :raises ShowNotFoundError: If the show is not found.
        """
        db_show = self.db.get(Show, show_id)
        if not db_show:
            raise ShowNotFoundError(show_id)

        stmt = (
            select(Season)
            .where(Season.show_id == show_id)
            .where(Season.number == season_data.number)
        )
        existing_db_season = self.db.execute(stmt).scalar_one_or_none()
        if existing_db_season:
            return SeasonSchema.model_validate(existing_db_season)

        db_season = Season(
            id=season_data.id,
            show_id=show_id,
            number=season_data.number,
            external_id=season_data.external_id,
            name=season_data.name,
            overview=season_data.overview,
            episodes=[
                Episode(
                    id=ep_schema.id,
                    number=ep_schema.number,
                    external_id=ep_schema.external_id,
                    title=ep_schema.title,
                )
                for ep_schema in season_data.episodes
            ],
        )

        with self.db.begin():
            self.db.add(db_season)
        self.db.refresh(db_season)
        return SeasonSchema.model_validate(db_season)

    def add_episode_to_season(
        self, season_id: SeasonId, episode_data: EpisodeSchema
    ) -> EpisodeSchema:
        """
        Adds a new episode to a season.
        If the episode number already exists for the season, it returns the existing episode.

        :param season_id: The ID of the season to add the episode to.
        :param episode_data: The EpisodeSchema object for the new episode.
        :return: The added or existing EpisodeSchema object.
        :raises SeasonNotFoundError: If the season is not found.
        """
        db_season = self.db.get(Season, season_id)
        if not db_season:
            raise SeasonNotFoundError(season_id)

        stmt = (
            select(Episode)
            .where(Episode.season_id == season_id)
            .where(Episode.number == episode_data.number)
        )
        existing_db_episode = self.db.execute(stmt).scalar_one_or_none()
        if existing_db_episode:
            return EpisodeSchema.model_validate(existing_db_episode)

        db_episode = Episode(
            id=episode_data.id,
            season_id=season_id,
            number=episode_data.number,
            external_id=episode_data.external_id,
            title=episode_data.title,
        )

        with self.db.begin():
            self.db.add(db_episode)
        self.db.refresh(db_episode)
        return EpisodeSchema.model_validate(db_episode)

    def update_show_attributes(
        self, show_id: ShowId, /, **kwargs: Unpack[ShowAttributes]
    ) -> ShowSchema:
        """
        Update attributes of an existing show.
        :return: The updated ShowSchema object.
        :raises ShowNotFoundError: If the show is not found.
        """
        db_show = self.db.get(Show, show_id)
        if not db_show:
            raise ShowNotFoundError(show_id)

        if _update_db_fields(db_show, kwargs):
            self.db.commit()
            self.db.refresh(db_show)
        return ShowSchema.model_validate(db_show)

    def update_season_attributes(
        self, season_id: SeasonId, /, **kwargs: Unpack[SeasonAttributes]
    ) -> SeasonSchema:
        """
        Update attributes of an existing season.

        :param season_id: The ID of the season to update.
        :return: The updated SeasonSchema object.
        :raises SeasonNotFoundError: If the season is not found.
        """
        db_season = self.db.get(Season, season_id)
        if not db_season:
            raise SeasonNotFoundError(season_id)

        if _update_db_fields(db_season, kwargs):
            self.db.commit()
            self.db.refresh(db_season)
        return SeasonSchema.model_validate(db_season)

    def update_episode_attributes(
        self, episode_id: EpisodeId, /, **kwargs: Unpack[EpisodeAttributes]
    ) -> EpisodeSchema:
        """
        Update attributes of an existing episode.

        :param overview: The new overview for the episode.
        :param episode_id: The ID of the episode to update.
        :return: The updated EpisodeSchema object.
        :raises EpisodeNotFoundError: If the episode is not found.
        """
        db_episode = self.db.get(Episode, episode_id)
        if not db_episode:
            raise EpisodeNotFoundError(episode_id)

        if _update_db_fields(db_episode, kwargs):
            self.db.commit()
            self.db.refresh(db_episode)
        return EpisodeSchema.model_validate(db_episode)
